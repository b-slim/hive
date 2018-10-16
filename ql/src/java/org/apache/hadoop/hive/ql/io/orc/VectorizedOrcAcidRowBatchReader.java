/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
/**
 * A fast vectorized batch reader class for ACID. Insert events are read directly
 * from the base files/insert_only deltas in vectorized row batches. The deleted
 * rows can then be easily indicated via the 'selected' field of the vectorized row batch.
 * Refer HIVE-14233 for more details.
 */
public class VectorizedOrcAcidRowBatchReader
    implements org.apache.hadoop.mapred.RecordReader<NullWritable,VectorizedRowBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedOrcAcidRowBatchReader.class);

  private org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader;
  private final VectorizedRowBatchCtx rbCtx;
  private VectorizedRowBatch vectorizedRowBatchBase;
  private long offset;
  private long length;
  protected float progress = 0.0f;
  protected Object[] partitionValues;
  private boolean addPartitionCols = true;
  /**
   * true means there is no OrcRecordUpdater.ROW column
   * (i.e. the struct wrapping user columns) in {@link #vectorizedRowBatchBase}.
   */
  private final boolean isFlatPayload;
  private final ValidWriteIdList validWriteIdList;
  private final DeleteEventRegistry deleteEventRegistry;
  /**
   * {@link RecordIdentifier}/{@link VirtualColumn#ROWID} information
   */
  private final StructColumnVector recordIdColumnVector;
  private final Reader.Options readerOptions;
  private final boolean isOriginal;
  /**
   * something further in the data pipeline wants {@link VirtualColumn#ROWID}
   */
  private final boolean rowIdProjected;
  /**
   * if false, we don't need any acid medadata columns from the file because we
   * know all data in the split is valid (wrt to visible writeIDs/delete events)
   * and ROW_ID is not needed higher up in the operator pipeline
   */
  private final boolean includeAcidColumns;
  /**
   * partition/table root
   */
  private final Path rootPath;
  /**
   * for reading "original" files
   */
  private final OrcSplit.OffsetAndBucketProperty syntheticProps;
  /**
   * To have access to {@link RecordReader#getRowNumber()} in the underlying file which we need to
   * generate synthetic ROW_IDs for original files
   */
  private RecordReader innerReader;
  /**
   * min/max ROW__ID for the split (if available) so that we can limit the
   * number of delete events to load in memory
   */
  private final OrcRawRecordMerger.KeyInterval keyInterval;
  /**
   * {@link SearchArgument} pushed down to delete_deltaS
   */
  private SearchArgument deleteEventSarg = null;
  //OrcInputFormat c'tor
  VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf,
                                  Reporter reporter) throws IOException {
    this(inputSplit, conf,reporter, null);
  }
  @VisibleForTesting
  VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf,
        Reporter reporter, VectorizedRowBatchCtx rbCtx) throws IOException {
    this(conf, inputSplit, reporter,
        rbCtx == null ? Utilities.getVectorizedRowBatchCtx(conf) : rbCtx, false);

    final Reader reader = OrcInputFormat.createOrcReaderForSplit(conf, inputSplit);
    // Careful with the range here now, we do not want to read the whole base file like deltas.
    innerReader = reader.rowsOptions(readerOptions.range(offset, length), conf);
    baseReader = new org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch>() {

      @Override
      public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
        return innerReader.nextBatch(value);
      }

      @Override
      public NullWritable createKey() {
        return NullWritable.get();
      }

      @Override
      public VectorizedRowBatch createValue() {
        return rbCtx.createVectorizedRowBatch();
      }

      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
        innerReader.close();
      }

      @Override
      public float getProgress() throws IOException {
        return innerReader.getProgress();
      }
    };
    final boolean useDecimal64ColumnVectors = HiveConf
      .getVar(conf, ConfVars.HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
    if (useDecimal64ColumnVectors) {
      this.vectorizedRowBatchBase = ((RecordReaderImpl) innerReader).createRowBatch(true);
    } else {
      this.vectorizedRowBatchBase = ((RecordReaderImpl) innerReader).createRowBatch(false);
    }
  }

  /**
   * LLAP IO c'tor
   */
  public VectorizedOrcAcidRowBatchReader(OrcSplit inputSplit, JobConf conf, Reporter reporter,
    org.apache.hadoop.mapred.RecordReader<NullWritable, VectorizedRowBatch> baseReader,
    VectorizedRowBatchCtx rbCtx, boolean isFlatPayload) throws IOException {
    this(conf, inputSplit, reporter, rbCtx, isFlatPayload);
    if (baseReader != null) {
      setBaseAndInnerReader(baseReader);
    }
  }

  private VectorizedOrcAcidRowBatchReader(JobConf conf, OrcSplit orcSplit, Reporter reporter,
      VectorizedRowBatchCtx rowBatchCtx, boolean isFlatPayload) throws IOException {
    this.isFlatPayload = isFlatPayload;
    this.rbCtx = rowBatchCtx;
    final boolean isAcidRead = AcidUtils.isFullAcidScan(conf);
    final AcidUtils.AcidOperationalProperties acidOperationalProperties
            = AcidUtils.getAcidOperationalProperties(conf);

    // This type of VectorizedOrcAcidRowBatchReader can only be created when split-update is
    // enabled for an ACID case and the file format is ORC.
    boolean isReadNotAllowed = !isAcidRead || !acidOperationalProperties.isSplitUpdate();
    if (isReadNotAllowed) {
      OrcInputFormat.raiseAcidTablesMustBeReadWithAcidReaderException(conf);
    }

    reporter.setStatus(orcSplit.toString());
    readerOptions = OrcInputFormat.createOptionsForReader(conf);

    this.offset = orcSplit.getStart();
    this.length = orcSplit.getLength();

    int partitionColumnCount = (rbCtx != null) ? rbCtx.getPartitionColumnCount() : 0;
    if (partitionColumnCount > 0) {
      partitionValues = new Object[partitionColumnCount];
      VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, orcSplit, partitionValues);
    } else {
      partitionValues = null;
    }

    String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
    this.validWriteIdList = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
    LOG.info("Read ValidWriteIdList: " + this.validWriteIdList.toString()
            + ":" + orcSplit);

    this.syntheticProps = orcSplit.getSyntheticAcidProps();

    // Clone readerOptions for deleteEvents.
    Reader.Options deleteEventReaderOptions = readerOptions.clone();
    // Set the range on the deleteEventReaderOptions to 0 to INTEGER_MAX because
    // we always want to read all the delete delta files.
    deleteEventReaderOptions.range(0, Long.MAX_VALUE);
    keyInterval = findMinMaxKeys(orcSplit, conf, deleteEventReaderOptions);
    DeleteEventRegistry der;
    try {
      // See if we can load all the relevant delete events from all the
      // delete deltas in memory...
      der = new ColumnizedDeleteEventRegistry(conf, orcSplit,
          deleteEventReaderOptions, keyInterval);
    } catch (DeleteEventsOverflowMemoryException e) {
      // If not, then create a set of hanging readers that do sort-merge to find the next smallest
      // delete event on-demand. Caps the memory consumption to (some_const * no. of readers).
      der = new SortMergedDeleteEventRegistry(conf, orcSplit, deleteEventReaderOptions);
    }
    this.deleteEventRegistry = der;
    isOriginal = orcSplit.isOriginal();
    if (isOriginal) {
      recordIdColumnVector = new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
        new LongColumnVector(), new LongColumnVector(), new LongColumnVector());
    } else {
      // Will swap in the Vectors from underlying row batch.
      recordIdColumnVector = new StructColumnVector(
          VectorizedRowBatch.DEFAULT_SIZE, null, null, null);
    }
    rowIdProjected = areRowIdsProjected(rbCtx);
    rootPath = orcSplit.getRootDir();

    if(conf.getBoolean(ConfVars.OPTIMIZE_ACID_META_COLUMNS.varname, true)) {
      /*figure out if we can skip reading acid metadata columns:
       * isOriginal - don't have meta columns - nothing to skip
       * there no relevant delete events && ROW__ID is not needed higher up (e.g.
       * this is not a delete statement)*/
      if (!isOriginal && deleteEventRegistry.isEmpty() && !rowIdProjected) {
        Path parent = orcSplit.getPath().getParent();
        while (parent != null && !rootPath.equals(parent)) {
          if (parent.getName().startsWith(AcidUtils.BASE_PREFIX)) {
            /**
             * The assumption here is that any base_x is filtered out by
             * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} so if we see it
             * here it's valid.
             * {@link AcidUtils#isValidBase(long, ValidWriteIdList, Path, FileSystem)} can check but
             * it makes a {@link FileSystem} call.  Should really move all this to split-generation...
             *
             */
            readerOptions.includeAcidColumns(false);
            break;
          } else {
            AcidUtils.ParsedDelta pd = AcidUtils.parsedDelta(parent, isOriginal);
            if (validWriteIdList.isWriteIdRangeValid(pd.getMinWriteId(), pd.getMaxWriteId()) ==
                ValidWriteIdList.RangeResponse.ALL) {
              //all write IDs in range are committed (and visible in current snapshot)
              readerOptions.includeAcidColumns(false);
              break;
            }
          }
          parent = parent.getParent();
        }
      }
    }
    includeAcidColumns = readerOptions.getIncludeAcidColumns();//default is true
  }

  /**
   * Generates a SearchArgument to push down to delete_delta files.
   *
   *
   * Note that bucket is a bit packed int, so even thought all delete events
   * for a given split have the same bucket ID but not the same "bucket" value
   * {@link BucketCodec}
   */
  private void setSARG(OrcRawRecordMerger.KeyInterval keyInterval,
      Reader.Options deleteEventReaderOptions,
      long minBucketProp, long maxBucketProp, long minRowId, long maxRowId) {
    SearchArgument.Builder b = null;
    if(keyInterval.getMinKey() != null) {
      RecordIdentifier k = keyInterval.getMinKey();
      b = SearchArgumentFactory.newBuilder();
      b.startAnd()  //not(ot < 7) -> ot >=7
          .startNot().lessThan("originalTransaction",
          PredicateLeaf.Type.LONG, k.getWriteId()).end();
      b.startNot().lessThan(
          "bucket", PredicateLeaf.Type.LONG, minBucketProp).end();
      b.startNot().lessThan("rowId",
          PredicateLeaf.Type.LONG, minRowId).end();
      b.end();
    }
    if(keyInterval.getMaxKey() != null) {
      RecordIdentifier k = keyInterval.getMaxKey();
      if(b == null) {
        b = SearchArgumentFactory.newBuilder();
      }
      b.startAnd().lessThanEquals(
          "originalTransaction", PredicateLeaf.Type.LONG, k.getWriteId());
      b.lessThanEquals("bucket", PredicateLeaf.Type.LONG, maxBucketProp);
      b.lessThanEquals("rowId", PredicateLeaf.Type.LONG, maxRowId);
      b.end();
    }
    if(b != null) {
      deleteEventSarg = b.build();
      LOG.info("deleteReader SARG(" + deleteEventSarg + ") ");
      deleteEventReaderOptions.searchArgument(deleteEventSarg,
          new String[] {"originalTransaction", "bucket", "rowId"});
      return;
    }
    deleteEventReaderOptions.searchArgument(null, null);
  }
  public boolean includeAcidColumns() {
    return this.includeAcidColumns;
  }
  public void setBaseAndInnerReader(
    final org.apache.hadoop.mapred.RecordReader<NullWritable,
        VectorizedRowBatch> baseReader) {
    this.baseReader = baseReader;
    this.innerReader = null;
    this.vectorizedRowBatchBase = baseReader.createValue();
  }

  /**
   * A given ORC reader will always process one or more whole stripes but the
   * split boundaries may not line up with stripe boundaries if the InputFormat
   * doesn't understand ORC specifics. So first we need to figure out which
   * stripe(s) we are reading.
   *
   * Suppose txn1 writes 100K rows
   * and txn2 writes 100 rows so we have events
   * {1,0,0}....{1,0,100K},{2,0,0}...{2,0,100} in 2 files
   * After compaction we may have 2 stripes
   * {1,0,0}...{1,0,90K},{1,0,90001}...{2,0,100}
   *
   * Now suppose there is a delete stmt that deletes every row.  So when we load
   * the 2nd stripe, if we just look at stripe {@link ColumnStatistics},
   * minKey={1,0,100} and maxKey={2,0,90001}, all but the 1st 100 delete events
   * will get loaded.  But with {@link OrcRecordUpdater#ACID_KEY_INDEX_NAME},
   * minKey={1,0,90001} and maxKey={2,0,100} so we only load about 10K deletes.
   *
   * Also, even with Query Based compactor (once we have it), FileSinkOperator
   * uses OrcRecordWriter to write to file, so we should have the
   * hive.acid.index in place.
   *
   * If reading the 1st stripe, we don't have the start event, so we'll get it
   * from stats, which will strictly speaking be accurate only wrt writeId and
   * bucket but that is good enough.
   *
   * @return empty <code>KeyInterval</code> if KeyInterval could not be
   * determined
   */
  private OrcRawRecordMerger.KeyInterval findMinMaxKeys(
      OrcSplit orcSplit, Configuration conf,
      Reader.Options deleteEventReaderOptions) throws IOException {
    if(!HiveConf.getBoolVar(conf, ConfVars.FILTER_DELETE_EVENTS)) {
      LOG.debug("findMinMaxKeys() " + ConfVars.FILTER_DELETE_EVENTS + "=false");
      return new OrcRawRecordMerger.KeyInterval(null, null);
    }

    //todo: since we already have OrcSplit.orcTail, should somehow use it to
    // get the acid.index, stats, etc rather than fetching the footer again
    // though it seems that orcTail is mostly null....
    Reader reader = OrcFile.createReader(orcSplit.getPath(),
        OrcFile.readerOptions(conf));

    if(orcSplit.isOriginal()) {
      /**
       * Among originals we may have files with _copy_N suffix.  To properly
       * generate a synthetic ROW___ID for them we need
       * {@link OffsetAndBucketProperty} which could be an expensive computation
       * if there are lots of copy_N files for a given bucketId. But unless
       * there are delete events, we often don't need synthetic ROW__IDs at all.
       * Kind of chicken-and-egg - deal with this later.
       * See {@link OrcRawRecordMerger#discoverOriginalKeyBounds(Reader, int,
       * Reader.Options, Configuration, OrcRawRecordMerger.Options)}*/
      LOG.debug("findMinMaxKeys(original split)");

      return findOriginalMinMaxKeys(orcSplit, reader, deleteEventReaderOptions);
    }

    List<StripeInformation> stripes = reader.getStripes();
    final long splitStart = orcSplit.getStart();
    final long splitEnd = splitStart + orcSplit.getLength();
    int firstStripeIndex = -1;
    int lastStripeIndex = -1;
    for(int i = 0; i < stripes.size(); i++) {
      StripeInformation stripe = stripes.get(i);
      long stripeEnd = stripe.getOffset() + stripe.getLength();
      if(firstStripeIndex == -1 && stripe.getOffset() >= splitStart) {
        firstStripeIndex = i;
      }
      if(lastStripeIndex == -1 && splitEnd <= stripeEnd) {
        lastStripeIndex = i;
      }
    }
    if(lastStripeIndex == -1) {
      //split goes to the EOF which is > end of stripe since file has a footer
      assert stripes.get(stripes.size() - 1).getOffset() +
          stripes.get(stripes.size() - 1).getLength() < splitEnd;
      lastStripeIndex = stripes.size() - 1;
    }

    if (firstStripeIndex > lastStripeIndex || firstStripeIndex == -1) {
      /**
       * If the firstStripeIndex was set after the lastStripeIndex the split lies entirely within a single stripe.
       * In case the split lies entirely within the last stripe, the firstStripeIndex will never be found, hence the
       * second condition.
       * In this case, the reader for this split will not read any data.
       * See {@link org.apache.orc.impl.RecordReaderImpl#RecordReaderImpl
       * Create a KeyInterval such that no delete delta records are loaded into memory in the deleteEventRegistry.
       */

      long minRowId = 1;
      long maxRowId = 0;
      int minBucketProp = 1;
      int maxBucketProp = 0;

      OrcRawRecordMerger.KeyInterval keyIntervalTmp =
          new OrcRawRecordMerger.KeyInterval(new RecordIdentifier(1, minBucketProp, minRowId),
          new RecordIdentifier(0, maxBucketProp, maxRowId));

      setSARG(keyIntervalTmp, deleteEventReaderOptions, minBucketProp, maxBucketProp,
          minRowId, maxRowId);
      LOG.info("findMinMaxKeys(): " + keyIntervalTmp +
          " stripes(" + firstStripeIndex + "," + lastStripeIndex + ")");

      return keyIntervalTmp;
    }

    if(firstStripeIndex == -1 || lastStripeIndex == -1) {
      //this should not happen but... if we don't know which stripe(s) are
      //involved we can't figure out min/max bounds
      LOG.warn("Could not find stripe (" + firstStripeIndex + "," +
          lastStripeIndex + ")");
      return new OrcRawRecordMerger.KeyInterval(null, null);
    }
    RecordIdentifier[] keyIndex = OrcRecordUpdater.parseKeyIndex(reader);
    if(keyIndex == null || keyIndex.length != stripes.size()) {
      LOG.warn("Could not find keyIndex or length doesn't match (" +
          firstStripeIndex + "," + lastStripeIndex + "," + stripes.size() + "," +
          (keyIndex == null ? -1 : keyIndex.length) + ")");
      return new OrcRawRecordMerger.KeyInterval(null, null);
    }
    /**
     * If {@link OrcConf.ROW_INDEX_STRIDE} is set to 0 all column stats on
     * ORC file are disabled though objects for them exist but and have
     * min/max set to MIN_LONG/MAX_LONG so we only use column stats if they
     * are actually computed.  Streaming ingest used to set it 0 and Minor
     * compaction so there are lots of legacy files with no (rather, bad)
     * column stats*/
    boolean columnStatsPresent = reader.getRowIndexStride() > 0;
    if(!columnStatsPresent) {
      LOG.debug("findMinMaxKeys() No ORC column stats");
    }
    RecordIdentifier minKey = null;
    if(firstStripeIndex > 0) {
      //valid keys are strictly > than this key
      minKey = keyIndex[firstStripeIndex - 1];
      //add 1 to make comparison >= to match the case of 0th stripe
      minKey.setRowId(minKey.getRowId() + 1);
    }
    else {
      List<StripeStatistics> stats = reader.getStripeStatistics();
      assert stripes.size() == stats.size() : "str.s=" + stripes.size() +
          " sta.s=" + stats.size();
      if(columnStatsPresent) {
        ColumnStatistics[] colStats =
            stats.get(firstStripeIndex).getColumnStatistics();
        /*
        Structure in data is like this:
         <op, owid, writerId, rowid, cwid, <f1, ... fn>>
        The +1 is to account for the top level struct which has a
        ColumnStatistics object in colsStats.  Top level struct is normally
        dropped by the Reader (I guess because of orc.impl.SchemaEvolution)
        */
        IntegerColumnStatistics origWriteId = (IntegerColumnStatistics)
            colStats[OrcRecordUpdater.ORIGINAL_WRITEID + 1];
        IntegerColumnStatistics bucketProperty = (IntegerColumnStatistics)
            colStats[OrcRecordUpdater.BUCKET + 1];
        IntegerColumnStatistics rowId = (IntegerColumnStatistics)
            colStats[OrcRecordUpdater.ROW_ID + 1];
        //we may want to change bucketProperty from int to long in the
        // future(across the stack) this protects the following cast to int
        assert bucketProperty.getMinimum() <= Integer.MAX_VALUE :
            "was bucketProper changed to a long (" +
                bucketProperty.getMinimum() + ")?!:" + orcSplit;
        //this a lower bound but not necessarily greatest lower bound
        minKey = new RecordIdentifier(origWriteId.getMinimum(),
            (int) bucketProperty.getMinimum(), rowId.getMinimum());
      }
    }
    OrcRawRecordMerger.KeyInterval keyInterval =
        new OrcRawRecordMerger.KeyInterval(minKey, keyIndex[lastStripeIndex]);
    LOG.info("findMinMaxKeys(): " + keyInterval +
        " stripes(" + firstStripeIndex + "," + lastStripeIndex + ")");

    long minBucketProp = Long.MAX_VALUE, maxBucketProp = Long.MIN_VALUE;
    long minRowId = Long.MAX_VALUE, maxRowId = Long.MIN_VALUE;
    if(columnStatsPresent) {
      /**
       * figure out min/max bucket, rowid for push down.  This is different from
       * min/max ROW__ID because ROW__ID comparison uses dictionary order on two
       * tuples (a,b,c), but PPD can only do
       * (a between (x,y) and b between(x1,y1) and c between(x2,y2))
       * Consider:
       * (0,536936448,0), (0,536936448,2), (10000001,536936448,0)
       * 1st is min ROW_ID, 3r is max ROW_ID
       * and Delete events (0,536936448,2),....,(10000001,536936448,1000000)
       * So PPD based on min/max ROW_ID would have 0<= rowId <=0 which will
       * miss this delete event.  But we still want PPD to filter out data if
       * possible.
       *
       * So use stripe stats to find proper min/max for bucketProp and rowId
       * writeId is the same in both cases
       */
      List<StripeStatistics> stats = reader.getStripeStatistics();
      for(int i = firstStripeIndex; i <= lastStripeIndex; i++) {
        ColumnStatistics[] colStats = stats.get(firstStripeIndex)
            .getColumnStatistics();
        IntegerColumnStatistics bucketProperty = (IntegerColumnStatistics)
            colStats[OrcRecordUpdater.BUCKET + 1];
        IntegerColumnStatistics rowId = (IntegerColumnStatistics)
            colStats[OrcRecordUpdater.ROW_ID + 1];
        if(bucketProperty.getMinimum() < minBucketProp) {
          minBucketProp = bucketProperty.getMinimum();
        }
        if(bucketProperty.getMaximum() > maxBucketProp) {
          maxBucketProp = bucketProperty.getMaximum();
        }
        if(rowId.getMinimum() < minRowId) {
          minRowId = rowId.getMinimum();
        }
        if(rowId.getMaximum() > maxRowId) {
          maxRowId = rowId.getMaximum();
        }
      }
    }
    if(minBucketProp == Long.MAX_VALUE) minBucketProp = Long.MIN_VALUE;
    if(maxBucketProp == Long.MIN_VALUE) maxBucketProp = Long.MAX_VALUE;
    if(minRowId == Long.MAX_VALUE) minRowId = Long.MIN_VALUE;
    if(maxRowId == Long.MIN_VALUE) maxRowId = Long.MAX_VALUE;

    setSARG(keyInterval, deleteEventReaderOptions, minBucketProp, maxBucketProp,
        minRowId, maxRowId);
    return keyInterval;
  }

  private OrcRawRecordMerger.KeyInterval findOriginalMinMaxKeys(OrcSplit orcSplit, Reader reader,
      Reader.Options deleteEventReaderOptions) {

    // This method returns the minimum and maximum synthetic row ids that are present in this split
    // because min and max keys are both inclusive when filtering out the delete delta records.

    if (syntheticProps == null) {
      // syntheticProps containing the synthetic rowid offset is computed if there are delete delta files.
      // If there aren't any delete delta files, then we don't need this anyway.
      return new OrcRawRecordMerger.KeyInterval(null, null);
    }

    long splitStart = orcSplit.getStart();
    long splitEnd = orcSplit.getStart() + orcSplit.getLength();

    long minRowId = syntheticProps.getRowIdOffset();
    long maxRowId = syntheticProps.getRowIdOffset();

    for(StripeInformation stripe: reader.getStripes()) {
      if (splitStart > stripe.getOffset()) {
        // This stripe starts before the current split starts. This stripe is not included in this split.
        minRowId += stripe.getNumberOfRows();
      }

      if (splitEnd > stripe.getOffset()) {
        // This stripe starts before the current split ends.
        maxRowId += stripe.getNumberOfRows();
      } else {
        // The split ends before (or exactly where) this stripe starts.
        // Remaining stripes are not included in this split.
        break;
      }
    }

    RecordIdentifier minKey = new RecordIdentifier(syntheticProps.getSyntheticWriteId(),
        syntheticProps.getBucketProperty(), minRowId);

    RecordIdentifier maxKey = new RecordIdentifier(syntheticProps.getSyntheticWriteId(),
        syntheticProps.getBucketProperty(), maxRowId > 0? maxRowId - 1: 0);

    OrcRawRecordMerger.KeyInterval keyIntervalTmp = new OrcRawRecordMerger.KeyInterval(minKey, maxKey);

    if (minRowId >= maxRowId) {
      /**
       * The split lies entirely within a single stripe. In this case, the reader for this split will not read any data.
       * See {@link org.apache.orc.impl.RecordReaderImpl#RecordReaderImpl
       * We can return the min max key interval as is (it will not read any of the delete delta records into mem)
       */

      LOG.info("findOriginalMinMaxKeys(): This split starts and ends in the same stripe.");
    }

    LOG.info("findOriginalMinMaxKeys(): " + keyIntervalTmp);

    // Using min/max ROW__ID from original will work for ppd to the delete deltas because the writeid is the same in
    // the min and the max ROW__ID
    setSARG(keyIntervalTmp, deleteEventReaderOptions, minKey.getBucketProperty(), maxKey.getBucketProperty(),
        minKey.getRowId(), maxKey.getRowId());

    return keyIntervalTmp;
  }

  /**
   * See {@link #next(NullWritable, VectorizedRowBatch)} first and
   * {@link OrcRawRecordMerger.OriginalReaderPair}.
   * When reading a split of an "original" file and we need to decorate data with ROW__ID.
   * This requires treating multiple files that are part of the same bucket (tranche for unbucketed
   * tables) as a single logical file to number rowids consistently.
   */
  static OrcSplit.OffsetAndBucketProperty computeOffsetAndBucket(
          FileStatus file, Path rootDir, boolean isOriginal, boolean hasDeletes,
          Configuration conf) throws IOException {

    VectorizedRowBatchCtx vrbCtx = Utilities.getVectorizedRowBatchCtx(conf);

    if (!needSyntheticRowIds(isOriginal, hasDeletes, areRowIdsProjected(vrbCtx))) {
      if(isOriginal) {
        /**
         * Even if we don't need to project ROW_IDs, we still need to check the write ID that
         * created the file to see if it's committed.  See more in
         * {@link #next(NullWritable, VectorizedRowBatch)}.  (In practice getAcidState() should
         * filter out base/delta files but this makes fewer dependencies)
         */
        OrcRawRecordMerger.TransactionMetaData syntheticTxnInfo =
            OrcRawRecordMerger.TransactionMetaData.findWriteIDForSynthetcRowIDs(file.getPath(),
                    rootDir, conf);
        return new OrcSplit.OffsetAndBucketProperty(-1, -1, syntheticTxnInfo.syntheticWriteId);
      }
      return null;
    }

    String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
    ValidWriteIdList validWriteIdList = (txnString == null) ? new ValidReaderWriteIdList() :
        new ValidReaderWriteIdList(txnString);

    long rowIdOffset = 0;
    OrcRawRecordMerger.TransactionMetaData syntheticTxnInfo =
        OrcRawRecordMerger.TransactionMetaData.findWriteIDForSynthetcRowIDs(file.getPath(), rootDir, conf);
    int bucketId = AcidUtils.parseBucketId(file.getPath());
    int bucketProperty = BucketCodec.V1.encode(new AcidOutputFormat.Options(conf)
        //statementId is from directory name (or 0 if there is none)
      .statementId(syntheticTxnInfo.statementId).bucket(bucketId));
    AcidUtils.Directory directoryState = AcidUtils.getAcidState( syntheticTxnInfo.folder, conf,
        validWriteIdList, false, true);
    for (HadoopShims.HdfsFileStatusWithId f : directoryState.getOriginalFiles()) {
      int bucketIdFromPath = AcidUtils.parseBucketId(f.getFileStatus().getPath());
      if (bucketIdFromPath != bucketId) {
        continue;//HIVE-16952
      }
      if (f.getFileStatus().getPath().equals(file.getPath())) {
        //'f' is the file whence this split is
        break;
      }
      Reader reader = OrcFile.createReader(f.getFileStatus().getPath(),
        OrcFile.readerOptions(conf));
      rowIdOffset += reader.getNumberOfRows();
    }
    return new OrcSplit.OffsetAndBucketProperty(rowIdOffset, bucketProperty,
      syntheticTxnInfo.syntheticWriteId);
  }
  /**
   * {@link VectorizedOrcAcidRowBatchReader} is always used for vectorized reads of acid tables.
   * In some cases this cannot be used from LLAP IO elevator because
   * {@link RecordReader#getRowNumber()} is not (currently) available there but is required to
   * generate ROW__IDs for "original" files
   * @param hasDeletes - if there are any deletes that apply to this split
   * todo: HIVE-17944
   */
  static boolean canUseLlapForAcid(OrcSplit split, boolean hasDeletes, Configuration conf) {
    if(!split.isOriginal()) {
      return true;
    }
    VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
    if(rbCtx == null) {
      throw new IllegalStateException("Could not create VectorizedRowBatchCtx for " + split.getPath());
    }
    return !needSyntheticRowIds(split.isOriginal(), hasDeletes, areRowIdsProjected(rbCtx));
  }

  /**
   * Does this reader need to decorate rows with ROW__IDs (for "original" reads).
   * Even if ROW__ID is not projected you still need to decorate the rows with them to see if
   * any of the delete events apply.
   */
  private static boolean needSyntheticRowIds(boolean isOriginal, boolean hasDeletes, boolean rowIdProjected) {
    return isOriginal && (hasDeletes || rowIdProjected);
  }

  private static boolean areRowIdsProjected(VectorizedRowBatchCtx rbCtx) {
    if (rbCtx.getVirtualColumnCount() == 0) {
      return false;
    }
    for(VirtualColumn vc : rbCtx.getNeededVirtualColumns()) {
      if(vc == VirtualColumn.ROWID) {
        //The query needs ROW__ID: maybe explicitly asked, maybe it's part of
        // Update/Delete statement.
        //Either way, we need to decorate "original" rows with row__id
        return true;
      }
    }
    return false;
  }
  static Path[] getDeleteDeltaDirsFromSplit(OrcSplit orcSplit) throws IOException {
    Path path = orcSplit.getPath();
    Path root;
    if (orcSplit.hasBase()) {
      if (orcSplit.isOriginal()) {
        root = orcSplit.getRootDir();
      } else {
        root = path.getParent().getParent();//todo: why not just use getRootDir()?
        assert root.equals(orcSplit.getRootDir()) : "root mismatch: baseDir=" + orcSplit.getRootDir() +
          " path.p.p=" + root;
      }
    } else {
      throw new IllegalStateException("Split w/o base w/Acid 2.0??: " + path);
    }
    return AcidUtils.deserializeDeleteDeltas(root, orcSplit.getDeltas());
  }

  /**
   * There are 2 types of schema from the {@link #baseReader} that this handles.  In the case
   * the data was written to a transactional table from the start, every row is decorated with
   * transaction related info and looks like <op, owid, writerId, rowid, cwid, <f1, ... fn>>.
   *
   * The other case is when data was written to non-transactional table and thus only has the user
   * data: <f1, ... fn>.  Then this table was then converted to a transactional table but the data
   * files are not changed until major compaction.  These are the "original" files.
   *
   * In this case we may need to decorate the outgoing data with transactional column values at
   * read time.  (It's done somewhat out of band via VectorizedRowBatchCtx - ask Teddy Choi).
   * The "owid, writerId, rowid" columns represent {@link RecordIdentifier}.  They are assigned
   * each time the table is read in a way that needs to project {@link VirtualColumn#ROWID}.
   * Major compaction will attach these values to each row permanently.
   * It's critical that these generated column values are assigned exactly the same way by each
   * read of the same row and by the Compactor.
   * See {@link org.apache.hadoop.hive.ql.txn.compactor.CompactorMR} and
   * {@link OrcRawRecordMerger.OriginalReaderPairToCompact} for the Compactor read path.
   * (Longer term should make compactor use this class)
   *
   * This only decorates original rows with metadata if something above is requesting these values
   * or if there are Delete events to apply.
   *
   * @return false where there is no more data, i.e. {@code value} is empty
   */
  @Override
  public boolean next(NullWritable key, VectorizedRowBatch value) throws IOException {
    try {
      // Check and update partition cols if necessary. Ideally, this should be done
      // in CreateValue as the partition is constant per split. But since Hive uses
      // CombineHiveRecordReader and
      // as this does not call CreateValue for each new RecordReader it creates, this check is
      // required in next()
      if (addPartitionCols) {
        if (partitionValues != null) {
          rbCtx.addPartitionColsToBatch(value, partitionValues);
        }
        addPartitionCols = false;
      }
      if (!baseReader.next(null, vectorizedRowBatchBase)) {
        return false;
      }
    } catch (Exception e) {
      throw new IOException("error iterating", e);
    }
    if(!includeAcidColumns) {
      //if here, we don't need to filter anything wrt acid metadata columns
      //in fact, they are not even read from file/llap
      value.size = vectorizedRowBatchBase.size;
      value.selected = vectorizedRowBatchBase.selected;
      value.selectedInUse = vectorizedRowBatchBase.selectedInUse;
      copyFromBase(value);
      progress = baseReader.getProgress();
      return true;
    }
    // Once we have read the VectorizedRowBatchBase from the file, there are two kinds of cases
    // for which we might have to discard rows from the batch:
    // Case 1- when the row is created by a transaction that is not valid, or
    // Case 2- when the row has been deleted.
    // We will go through the batch to discover rows which match any of the cases and specifically
    // remove them from the selected vector. Of course, selectedInUse should also be set.

    BitSet selectedBitSet = new BitSet(vectorizedRowBatchBase.size);
    if (vectorizedRowBatchBase.selectedInUse) {
      // When selectedInUse is true, start with every bit set to false and selectively set
      // certain bits to true based on the selected[] vector.
      selectedBitSet.set(0, vectorizedRowBatchBase.size, false);
      for (int j = 0; j < vectorizedRowBatchBase.size; ++j) {
        int i = vectorizedRowBatchBase.selected[j];
        selectedBitSet.set(i);
      }
    } else {
      // When selectedInUse is set to false, everything in the batch is selected.
      selectedBitSet.set(0, vectorizedRowBatchBase.size, true);
    }
    ColumnVector[] innerRecordIdColumnVector = vectorizedRowBatchBase.cols;
    if (isOriginal) {
      // Handle synthetic row IDs for the original files.
      innerRecordIdColumnVector = handleOriginalFile(selectedBitSet, innerRecordIdColumnVector);
    } else {
      // Case 1- find rows which belong to write Ids that are not valid.
      findRecordsWithInvalidWriteIds(vectorizedRowBatchBase, selectedBitSet);
    }

    // Case 2- find rows which have been deleted.
    this.deleteEventRegistry.findDeletedRecords(innerRecordIdColumnVector,
        vectorizedRowBatchBase.size, selectedBitSet);

    if (selectedBitSet.cardinality() == vectorizedRowBatchBase.size) {
      // None of the cases above matched and everything is selected. Hence, we will use the
      // same values for the selected and selectedInUse.
      value.size = vectorizedRowBatchBase.size;
      value.selected = vectorizedRowBatchBase.selected;
      value.selectedInUse = vectorizedRowBatchBase.selectedInUse;
    } else {
      value.size = selectedBitSet.cardinality();
      value.selectedInUse = true;
      value.selected = new int[selectedBitSet.cardinality()];
      // This loop fills up the selected[] vector with all the index positions that are selected.
      for (int setBitIndex = selectedBitSet.nextSetBit(0), selectedItr = 0;
           setBitIndex >= 0;
           setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1), ++selectedItr) {
        value.selected[selectedItr] = setBitIndex;
      }
    }

    if (isOriginal) {
     /* Just copy the payload.  {@link recordIdColumnVector} has already been populated */
      System.arraycopy(vectorizedRowBatchBase.cols, 0, value.cols, 0, value.getDataColumnCount());
    } else {
      copyFromBase(value);
    }
    if (rowIdProjected) {
      int ix = rbCtx.findVirtualColumnNum(VirtualColumn.ROWID);
      value.cols[ix] = recordIdColumnVector;
    }
    progress = baseReader.getProgress();
    return true;
  }
  //get the 'data' cols and set in value as individual ColumnVector, then get ColumnVectors for acid meta cols to create a single ColumnVector representing RecordIdentifier and (optionally) set it in 'value'
  private void copyFromBase(VectorizedRowBatch value) {
    assert !isOriginal;
    if (isFlatPayload) {
      int payloadCol = includeAcidColumns ? OrcRecordUpdater.ROW : 0;
        // Ignore the struct column and just copy all the following data columns.
        System.arraycopy(vectorizedRowBatchBase.cols, payloadCol + 1, value.cols, 0,
            vectorizedRowBatchBase.cols.length - payloadCol - 1);
    } else {
      StructColumnVector payloadStruct =
          (StructColumnVector) vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW];
      // Transfer columnVector objects from base batch to outgoing batch.
      System.arraycopy(payloadStruct.fields, 0, value.cols, 0, value.getDataColumnCount());
    }
    if (rowIdProjected) {
      recordIdColumnVector.fields[0] = vectorizedRowBatchBase.cols[OrcRecordUpdater.ORIGINAL_WRITEID];
      recordIdColumnVector.fields[1] = vectorizedRowBatchBase.cols[OrcRecordUpdater.BUCKET];
      recordIdColumnVector.fields[2] = vectorizedRowBatchBase.cols[OrcRecordUpdater.ROW_ID];
    }
  }
  private ColumnVector[] handleOriginalFile(
      BitSet selectedBitSet, ColumnVector[] innerRecordIdColumnVector) throws IOException {
    /*
     * If there are deletes and reading original file, we must produce synthetic ROW_IDs in order
     * to see if any deletes apply
     */
    boolean needSyntheticRowId =
        needSyntheticRowIds(true, !deleteEventRegistry.isEmpty(), rowIdProjected);
    if(needSyntheticRowId) {
      assert syntheticProps != null : "" + syntheticProps;
      assert syntheticProps.getRowIdOffset() >= 0 : "" + syntheticProps;
      assert syntheticProps.getBucketProperty() >= 0 : "" + syntheticProps;
      if(innerReader == null) {
        throw new IllegalStateException(getClass().getName() + " requires " +
          org.apache.orc.RecordReader.class +
          " to handle original files that require ROW__IDs: " + rootPath);
      }
      /**
       * {@link RecordIdentifier#getWriteId()}
       */
      recordIdColumnVector.fields[0].noNulls = true;
      recordIdColumnVector.fields[0].isRepeating = true;
      ((LongColumnVector)recordIdColumnVector.fields[0]).vector[0] = syntheticProps.getSyntheticWriteId();
      /**
       * This is {@link RecordIdentifier#getBucketProperty()}
       * Also see {@link BucketCodec}
       */
      recordIdColumnVector.fields[1].noNulls = true;
      recordIdColumnVector.fields[1].isRepeating = true;
      ((LongColumnVector)recordIdColumnVector.fields[1]).vector[0] = syntheticProps.getBucketProperty();
      /**
       * {@link RecordIdentifier#getRowId()}
       */
      recordIdColumnVector.fields[2].noNulls = true;
      recordIdColumnVector.fields[2].isRepeating = false;
      long[] rowIdVector = ((LongColumnVector)recordIdColumnVector.fields[2]).vector;
      for(int i = 0; i < vectorizedRowBatchBase.size; i++) {
        //baseReader.getRowNumber() seems to point at the start of the batch todo: validate
        rowIdVector[i] = syntheticProps.getRowIdOffset() + innerReader.getRowNumber() + i;
      }
      //Now populate a structure to use to apply delete events
      innerRecordIdColumnVector = new ColumnVector[OrcRecordUpdater.FIELDS];
      innerRecordIdColumnVector[OrcRecordUpdater.ORIGINAL_WRITEID] = recordIdColumnVector.fields[0];
      innerRecordIdColumnVector[OrcRecordUpdater.BUCKET] = recordIdColumnVector.fields[1];
      innerRecordIdColumnVector[OrcRecordUpdater.ROW_ID] = recordIdColumnVector.fields[2];
      //these are insert events so (original txn == current) txn for all rows
      innerRecordIdColumnVector[OrcRecordUpdater.CURRENT_WRITEID] = recordIdColumnVector.fields[0];
    }
    if(syntheticProps.getSyntheticWriteId() > 0) {
      //"originals" (written before table was converted to acid) is considered written by
      // writeid:0 which is always committed so there is no need to check wrt invalid write Ids
      //But originals written by Load Data for example can be in base_x or delta_x_x so we must
      //check if 'x' is committed or not evn if ROW_ID is not needed in the Operator pipeline.
      if (needSyntheticRowId) {
        findRecordsWithInvalidWriteIds(innerRecordIdColumnVector,
            vectorizedRowBatchBase.size, selectedBitSet);
      } else {
        /*since ROW_IDs are not needed we didn't create the ColumnVectors to hold them but we
        * still have to check if the data being read is committed as far as current
        * reader (transactions) is concerned.  Since here we are reading 'original' schema file,
        * all rows in it have been created by the same txn, namely 'syntheticProps.syntheticWriteId'
        */
        if (!validWriteIdList.isWriteIdValid(syntheticProps.getSyntheticWriteId())) {
          selectedBitSet.clear(0, vectorizedRowBatchBase.size);
        }
      }
    }
    return innerRecordIdColumnVector;
  }

  private void findRecordsWithInvalidWriteIds(VectorizedRowBatch batch, BitSet selectedBitSet) {
    findRecordsWithInvalidWriteIds(batch.cols, batch.size, selectedBitSet);
  }

  private void findRecordsWithInvalidWriteIds(ColumnVector[] cols, int size, BitSet selectedBitSet) {
    if (cols[OrcRecordUpdater.CURRENT_WRITEID].isRepeating) {
      // When we have repeating values, we can unset the whole bitset at once
      // if the repeating value is not a valid write id.
      long currentWriteIdForBatch = ((LongColumnVector)
          cols[OrcRecordUpdater.CURRENT_WRITEID]).vector[0];
      if (!validWriteIdList.isWriteIdValid(currentWriteIdForBatch)) {
        selectedBitSet.clear(0, size);
      }
      return;
    }
    long[] currentWriteIdVector =
        ((LongColumnVector) cols[OrcRecordUpdater.CURRENT_WRITEID]).vector;
    // Loop through the bits that are set to true and mark those rows as false, if their
    // current write ids are not valid.
    for (int setBitIndex = selectedBitSet.nextSetBit(0);
        setBitIndex >= 0;
        setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
      if (!validWriteIdList.isWriteIdValid(currentWriteIdVector[setBitIndex])) {
        selectedBitSet.clear(setBitIndex);
      }
   }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    return offset + (long) (progress * length);
  }

  @Override
  public void close() throws IOException {
    try {
      this.baseReader.close();
    } finally {
      this.deleteEventRegistry.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return progress;
  }

  @VisibleForTesting
  DeleteEventRegistry getDeleteEventRegistry() {
    return deleteEventRegistry;
  }

  /**
   * An interface that can determine which rows have been deleted
   * from a given vectorized row batch. Implementations of this interface
   * will read the delete delta files and will create their own internal
   * data structures to maintain record ids of the records that got deleted.
   */
  protected interface DeleteEventRegistry {
    /**
     * Modifies the passed bitset to indicate which of the rows in the batch
     * have been deleted. Assumes that the batch.size is equal to bitset size.
     * @param cols
     * @param size
     * @param selectedBitSet
     * @throws IOException
     */
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet) throws IOException;

    /**
     * The close() method can be called externally to signal the implementing classes
     * to free up resources.
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * @return {@code true} if no delete events were found
     */
    boolean isEmpty();
  }

  /**
   * An implementation for DeleteEventRegistry that opens the delete delta files all
   * at once, and then uses the sort-merge algorithm to maintain a sorted list of
   * delete events. This internally uses the OrcRawRecordMerger and maintains a constant
   * amount of memory usage, given the number of delete delta files. Therefore, this
   * implementation will be picked up when the memory pressure is high.
   *
   * Don't bother to use KeyInterval from split here because since this doesn't
   * buffer delete events in memory.
   */
  static class SortMergedDeleteEventRegistry implements DeleteEventRegistry {
    private OrcRawRecordMerger deleteRecords;
    private OrcRawRecordMerger.ReaderKey deleteRecordKey;
    private OrcStruct deleteRecordValue;
    private Boolean isDeleteRecordAvailable = null;
    private ValidWriteIdList validWriteIdList;

    SortMergedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit,
        Reader.Options readerOptions) throws IOException {
      final Path[] deleteDeltas = getDeleteDeltaDirsFromSplit(orcSplit);
      if (deleteDeltas.length > 0) {
        int bucket = AcidUtils.parseBucketId(orcSplit.getPath());
        String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
        this.validWriteIdList
                = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
        LOG.debug("Using SortMergedDeleteEventRegistry");
        OrcRawRecordMerger.Options mergerOptions = new OrcRawRecordMerger.Options().isDeleteReader(true);
        assert !orcSplit.isOriginal() : "If this now supports Original splits, set up mergeOptions properly";
        this.deleteRecords = new OrcRawRecordMerger(conf, true, null, false, bucket,
                                                    validWriteIdList, readerOptions, deleteDeltas,
                                                    mergerOptions);
        this.deleteRecordKey = new OrcRawRecordMerger.ReaderKey();
        this.deleteRecordValue = this.deleteRecords.createValue();
        // Initialize the first value in the delete reader.
        this.isDeleteRecordAvailable = this.deleteRecords.next(deleteRecordKey, deleteRecordValue);
      } else {
        this.isDeleteRecordAvailable = false;
        this.deleteRecordKey = null;
        this.deleteRecordValue = null;
        this.deleteRecords = null;
      }
    }

    @Override
    public boolean isEmpty() {
      if(isDeleteRecordAvailable == null) {
        throw new IllegalStateException("Not yet initialized");
      }
      return !isDeleteRecordAvailable;
    }
    @Override
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet)
        throws IOException {
      if (!isDeleteRecordAvailable) {
        return;
      }

      long[] originalWriteId =
          cols[OrcRecordUpdater.ORIGINAL_WRITEID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector;
      long[] bucket =
          cols[OrcRecordUpdater.BUCKET].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector;
      long[] rowId =
          cols[OrcRecordUpdater.ROW_ID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector;

      // The following repeatedX values will be set, if any of the columns are repeating.
      long repeatedOriginalWriteId = (originalWriteId != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector[0];
      long repeatedBucket = (bucket != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector[0];
      long repeatedRowId = (rowId != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector[0];


      // Get the first valid row in the batch still available.
      int firstValidIndex = selectedBitSet.nextSetBit(0);
      if (firstValidIndex == -1) {
        return; // Everything in the batch has already been filtered out.
      }
      RecordIdentifier firstRecordIdInBatch =
          new RecordIdentifier(
              originalWriteId != null ? originalWriteId[firstValidIndex] : repeatedOriginalWriteId,
              bucket != null ? (int) bucket[firstValidIndex] : (int) repeatedBucket,
              rowId != null ? (int)  rowId[firstValidIndex] : repeatedRowId);

      // Get the last valid row in the batch still available.
      int lastValidIndex = selectedBitSet.previousSetBit(size - 1);
      RecordIdentifier lastRecordIdInBatch =
          new RecordIdentifier(
              originalWriteId != null ? originalWriteId[lastValidIndex] : repeatedOriginalWriteId,
              bucket != null ? (int) bucket[lastValidIndex] : (int) repeatedBucket,
              rowId != null ? (int)  rowId[lastValidIndex] : repeatedRowId);

      // We must iterate over all the delete records, until we find one record with
      // deleteRecord >= firstRecordInBatch or until we exhaust all the delete records.
      while (deleteRecordKey.compareRow(firstRecordIdInBatch) == -1) {
        isDeleteRecordAvailable = deleteRecords.next(deleteRecordKey, deleteRecordValue);
        if (!isDeleteRecordAvailable) return; // exhausted all delete records, return.
      }

      // If we are here, then we have established that firstRecordInBatch <= deleteRecord.
      // Now continue marking records which have been deleted until we reach the end of the batch
      // or we exhaust all the delete records.

      int currIndex = firstValidIndex;
      RecordIdentifier currRecordIdInBatch = new RecordIdentifier();
      while (isDeleteRecordAvailable && currIndex != -1 && currIndex <= lastValidIndex) {
        currRecordIdInBatch.setValues(
            (originalWriteId != null) ? originalWriteId[currIndex] : repeatedOriginalWriteId,
            (bucket != null) ? (int) bucket[currIndex] : (int) repeatedBucket,
            (rowId != null) ? rowId[currIndex] : repeatedRowId);

        if (deleteRecordKey.compareRow(currRecordIdInBatch) == 0) {
          // When deleteRecordId == currRecordIdInBatch, this record in the batch has been deleted.
          selectedBitSet.clear(currIndex);
          currIndex = selectedBitSet.nextSetBit(currIndex + 1); // Move to next valid index.
        } else if (deleteRecordKey.compareRow(currRecordIdInBatch) == 1) {
          // When deleteRecordId > currRecordIdInBatch, we have to move on to look at the
          // next record in the batch.
          // But before that, can we short-circuit and skip the entire batch itself
          // by checking if the deleteRecordId > lastRecordInBatch?
          if (deleteRecordKey.compareRow(lastRecordIdInBatch) == 1) {
            return; // Yay! We short-circuited, skip everything remaining in the batch and return.
          }
          currIndex = selectedBitSet.nextSetBit(currIndex + 1); // Move to next valid index.
        } else {
          // We have deleteRecordId < currRecordIdInBatch, we must now move on to find
          // next the larger deleteRecordId that can possibly match anything in the batch.
          isDeleteRecordAvailable = deleteRecords.next(deleteRecordKey, deleteRecordValue);
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (this.deleteRecords != null) {
        this.deleteRecords.close();
      }
    }
  }

  /**
   * An implementation for DeleteEventRegistry that optimizes for performance by loading
   * all the delete events into memory at once from all the delete delta files.
   * It starts by reading all the delete events through a regular sort merge logic
   * into 3 vectors- one for original Write id (owid), one for bucket property and one for
   * row id.  See {@link BucketCodec} for more about bucket property.
   * The owids are likely to be repeated very often, as a single transaction
   * often deletes thousands of rows. Hence, the owid vector is compressed to only store the
   * toIndex and fromIndex ranges in the larger row id vector. Now, querying whether a
   * record id is deleted or not, is done by performing a binary search on the
   * compressed owid range. If a match is found, then a binary search is then performed on
   * the larger rowId vector between the given toIndex and fromIndex. Of course, there is rough
   * heuristic that prevents creation of an instance of this class if the memory pressure is high.
   * The SortMergedDeleteEventRegistry is then the fallback method for such scenarios.
   */
   static class ColumnizedDeleteEventRegistry implements DeleteEventRegistry {
    /**
     * A simple wrapper class to hold the (owid, bucketProperty, rowId) pair.
     */
    static class DeleteRecordKey implements Comparable<DeleteRecordKey> {
      private static final DeleteRecordKey otherKey = new DeleteRecordKey();
      private long originalWriteId;
      /**
       * see {@link BucketCodec}
       */
      private int bucketProperty; 
      private long rowId;
      DeleteRecordKey() {
        this.originalWriteId = -1;
        this.rowId = -1;
      }
      public void set(long owid, int bucketProperty, long rowId) {
        this.originalWriteId = owid;
        this.bucketProperty = bucketProperty;
        this.rowId = rowId;
      }

      @Override
      public int compareTo(DeleteRecordKey other) {
        if (other == null) {
          return -1;
        }
        if (originalWriteId != other.originalWriteId) {
          return originalWriteId < other.originalWriteId ? -1 : 1;
        }
        if(bucketProperty != other.bucketProperty) {
          return bucketProperty < other.bucketProperty ? -1 : 1;
        }
        if (rowId != other.rowId) {
          return rowId < other.rowId ? -1 : 1;
        }
        return 0;
      }
      private int compareTo(RecordIdentifier other) {
        if (other == null) {
          return -1;
        }
        otherKey.set(other.getWriteId(), other.getBucketProperty(),
            other.getRowId());
        return compareTo(otherKey);
      }
      @Override
      public String toString() {
        return "DeleteRecordKey(" + originalWriteId + "," +
            RecordIdentifier.bucketToString(bucketProperty) + "," + rowId +")";
      }
    }

    /**
     * This class actually reads the delete delta files in vectorized row batches.
     * For every call to next(), it returns the next smallest record id in the file if available.
     * Internally, the next() buffers a row batch and maintains an index pointer, reading the
     * next batch when the previous batch is exhausted.
     *
     * For unbucketed tables this will currently return all delete events.  Once we trust that
     * the N in bucketN for "base" spit is reliable, all delete events not matching N can be skipped.
     */
    static class DeleteReaderValue {
      private VectorizedRowBatch batch;
      private final RecordReader recordReader;
      private int indexPtrInBatch;
      private final int bucketForSplit; // The bucket value should be same for all the records.
      private final ValidWriteIdList validWriteIdList;
      private boolean isBucketPropertyRepeating;
      private final boolean isBucketedTable;
      private final Reader reader;
      private final Path deleteDeltaFile;
      private final OrcRawRecordMerger.KeyInterval keyInterval;
      private final OrcSplit orcSplit;
      /**
       * total number in the file
       */
      private final long numEvents;
      /**
       * number of events lifted from disk
       * some may be skipped due to PPD
       */
      private long numEventsFromDisk = 0;
      /**
       * number of events actually loaded in memory
       */
      private long numEventsLoaded = 0;

      DeleteReaderValue(Reader deleteDeltaReader, Path deleteDeltaFile,
          Reader.Options readerOptions, int bucket, ValidWriteIdList validWriteIdList,
          boolean isBucketedTable, final JobConf conf,
          OrcRawRecordMerger.KeyInterval keyInterval, OrcSplit orcSplit)
          throws IOException {
        this.reader = deleteDeltaReader;
        this.deleteDeltaFile = deleteDeltaFile;
        this.recordReader  = deleteDeltaReader.rowsOptions(readerOptions, conf);
        this.bucketForSplit = bucket;
        final boolean useDecimal64ColumnVector = HiveConf.getVar(conf, ConfVars
          .HIVE_VECTORIZED_INPUT_FORMAT_SUPPORTS_ENABLED).equalsIgnoreCase("decimal_64");
        if (useDecimal64ColumnVector) {
          this.batch = deleteDeltaReader.getSchema().createRowBatchV2();
        } else {
          this.batch = deleteDeltaReader.getSchema().createRowBatch();
        }
        if (!recordReader.nextBatch(batch)) { // Read the first batch.
          this.batch = null; // Oh! the first batch itself was null. Close the reader.
        }
        this.indexPtrInBatch = 0;
        this.validWriteIdList = validWriteIdList;
        this.isBucketedTable = isBucketedTable;
        if(batch != null) {
          checkBucketId();//check 1st batch
        }
        this.keyInterval = keyInterval;
        this.orcSplit = orcSplit;
        this.numEvents = deleteDeltaReader.getNumberOfRows();
        LOG.debug("Num events stats({},x,x)", numEvents);
      }

      public boolean next(DeleteRecordKey deleteRecordKey) throws IOException {
        if (batch == null) {
          return false;
        }
        boolean isValidNext = false;
        while (!isValidNext) {
          if (indexPtrInBatch >= batch.size) {
            // We have exhausted our current batch, read the next batch.
            if (recordReader.nextBatch(batch)) {
              checkBucketId();
              indexPtrInBatch = 0; // After reading the batch, reset the pointer to beginning.
            } else {
              return false; // no more batches to read, exhausted the reader.
            }
          }
          long currentWriteId = setCurrentDeleteKey(deleteRecordKey);
          if(!isBucketPropertyRepeating) {
            checkBucketId(deleteRecordKey.bucketProperty);
          }
          ++indexPtrInBatch;
          numEventsFromDisk++;
          if(!isDeleteEventInRange(keyInterval, deleteRecordKey)) {
            continue;
          }
          if (validWriteIdList.isWriteIdValid(currentWriteId)) {
            isValidNext = true;
          }
        }
        numEventsLoaded++;
        return true;
      }
      static boolean isDeleteEventInRange(
          OrcRawRecordMerger.KeyInterval keyInterval,
          DeleteRecordKey deleteRecordKey) {
        if(keyInterval.getMinKey() != null &&
            deleteRecordKey.compareTo(keyInterval.getMinKey()) < 0) {
          //current deleteEvent is < than minKey
          return false;
        }
        if(keyInterval.getMaxKey() != null &&
            deleteRecordKey.compareTo(keyInterval.getMaxKey()) > 0) {
          //current deleteEvent is > than maxKey
          return false;
        }
        return true;
      }
      public void close() throws IOException {
        this.recordReader.close();
        LOG.debug("Num events stats({},{},{})",
            numEvents, numEventsFromDisk, numEventsLoaded);
      }
      private long setCurrentDeleteKey(DeleteRecordKey deleteRecordKey) {
        int originalWriteIdIndex =
          batch.cols[OrcRecordUpdater.ORIGINAL_WRITEID].isRepeating ? 0 : indexPtrInBatch;
        long originalWriteId
                = ((LongColumnVector) batch.cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector[originalWriteIdIndex];
        int bucketPropertyIndex =
          batch.cols[OrcRecordUpdater.BUCKET].isRepeating ? 0 : indexPtrInBatch;
        int bucketProperty = (int)((LongColumnVector)batch.cols[OrcRecordUpdater.BUCKET]).vector[bucketPropertyIndex];
        long rowId = ((LongColumnVector) batch.cols[OrcRecordUpdater.ROW_ID]).vector[indexPtrInBatch];
        int currentWriteIdIndex
                = batch.cols[OrcRecordUpdater.CURRENT_WRITEID].isRepeating ? 0 : indexPtrInBatch;
        long currentWriteId
                = ((LongColumnVector) batch.cols[OrcRecordUpdater.CURRENT_WRITEID]).vector[currentWriteIdIndex];
        deleteRecordKey.set(originalWriteId, bucketProperty, rowId);
        return currentWriteId;
      }
      private void checkBucketId() throws IOException {
        isBucketPropertyRepeating = batch.cols[OrcRecordUpdater.BUCKET].isRepeating;
        if(isBucketPropertyRepeating) {
          int bucketPropertyFromRecord = (int)((LongColumnVector)
            batch.cols[OrcRecordUpdater.BUCKET]).vector[0];
          checkBucketId(bucketPropertyFromRecord);
        }
      }
      /**
       * Whenever we are reading a batch, we must ensure that all the records in the batch
       * have the same bucket id as the bucket id of the split. If not, throw exception.
       */
      private void checkBucketId(int bucketPropertyFromRecord) throws IOException {
        int bucketIdFromRecord = BucketCodec.determineVersion(bucketPropertyFromRecord)
          .decodeWriterId(bucketPropertyFromRecord);
        if(bucketIdFromRecord != bucketForSplit) {
          DeleteRecordKey dummy = new DeleteRecordKey();
          setCurrentDeleteKey(dummy);
          throw new IOException("Corrupted records with different bucket ids "
              + "from the containing bucket file found! Expected bucket id "
              + bucketForSplit + ", however found " + dummy
              + ".  (" + orcSplit + "," + deleteDeltaFile + ")");
        }
      }

      @Override
      public String toString() {
        return "{reader=" + reader + ", isBucketPropertyRepeating=" + isBucketPropertyRepeating +
            ", bucketForSplit=" + bucketForSplit + ", isBucketedTable=" + isBucketedTable + "}";
      }
    }
    /**
     * A CompressedOwid class stores a compressed representation of the original
     * write ids (owids) read from the delete delta files. Since the record ids
     * are sorted by (owid, rowId) and owids are highly likely to be repetitive, it is
     * efficient to compress them as a CompressedOwid that stores the fromIndex and
     * the toIndex. These fromIndex and toIndex reference the larger vector formed by
     * concatenating the correspondingly ordered rowIds.
     */
    private final class CompressedOwid implements Comparable<CompressedOwid> {
      final long originalWriteId;
      final int bucketProperty;
      final int fromIndex; // inclusive
      int toIndex; // exclusive

      CompressedOwid(long owid, int bucketProperty, int fromIndex, int toIndex) {
        this.originalWriteId = owid;
        this.bucketProperty = bucketProperty;
        this.fromIndex = fromIndex;
        this.toIndex = toIndex;
      }

      @Override
      public int compareTo(CompressedOwid other) {
        // When comparing the CompressedOwid, the one with the lesser value is smaller.
        if (originalWriteId != other.originalWriteId) {
          return originalWriteId < other.originalWriteId ? -1 : 1;
        }
        if(bucketProperty != other.bucketProperty) {
          return bucketProperty < other.bucketProperty ? -1 : 1;
        }
        return 0;
      }
    }

    /**
     * Food for thought:
     * this is a bit problematic - in order to load ColumnizedDeleteEventRegistry we still open
     * all delete deltas at once - possibly causing OOM same as for {@link SortMergedDeleteEventRegistry}
     * which uses {@link OrcRawRecordMerger}.  Why not load all delete_delta sequentially.  Each
     * dd is sorted by {@link RecordIdentifier} so we could create a BTree like structure where the
     * 1st level is an array of originalWriteId where each entry points at an array
     * of bucketIds where each entry points at an array of rowIds.  We could probably use ArrayList
     * to manage insertion as the structure is built (LinkedList?).  This should reduce memory
     * footprint (as far as OrcReader to a single reader) - probably bad for LLAP IO
     * Or much simpler, make compaction of delete deltas very aggressive so that
     * we never have move than a few delete files to read.
     */
    private TreeMap<DeleteRecordKey, DeleteReaderValue> sortMerger;
    private long rowIds[];
    private CompressedOwid compressedOwids[];
    private ValidWriteIdList validWriteIdList;
    private Boolean isEmpty;
    private final int maxEventsInMemory;
    private final OrcSplit orcSplit;
    private final boolean testMode;



    ColumnizedDeleteEventRegistry(JobConf conf, OrcSplit orcSplit,
        Reader.Options readerOptions,
        OrcRawRecordMerger.KeyInterval keyInterval)
        throws IOException, DeleteEventsOverflowMemoryException {
      this.testMode = conf.getBoolean(ConfVars.HIVE_IN_TEST.varname, false);
      int bucket = AcidUtils.parseBucketId(orcSplit.getPath());
      String txnString = conf.get(ValidWriteIdList.VALID_WRITEIDS_KEY);
      this.validWriteIdList
              = (txnString == null) ? new ValidReaderWriteIdList() : new ValidReaderWriteIdList(txnString);
      LOG.debug("Using ColumnizedDeleteEventRegistry");
      this.sortMerger = new TreeMap<>();
      this.rowIds = null;
      this.compressedOwids = null;
      maxEventsInMemory = HiveConf
          .getIntVar(conf, ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY);
      final boolean isBucketedTable  = conf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0) > 0;
      this.orcSplit = orcSplit;

      try {
        final Path[] deleteDeltaDirs = getDeleteDeltaDirsFromSplit(orcSplit);
        if (deleteDeltaDirs.length > 0) {
          int totalDeleteEventCount = 0;
          for (Path deleteDeltaDir : deleteDeltaDirs) {
            FileSystem fs = deleteDeltaDir.getFileSystem(conf);
            Path[] deleteDeltaFiles = OrcRawRecordMerger.getDeltaFiles(deleteDeltaDir, bucket,
                new OrcRawRecordMerger.Options().isCompacting(false));
            for (Path deleteDeltaFile : deleteDeltaFiles) {
              // NOTE: Calling last flush length below is more for future-proofing when we have
              // streaming deletes. But currently we don't support streaming deletes, and this can
              // be removed if this becomes a performance issue.
              long length = OrcAcidUtils.getLastFlushLength(fs, deleteDeltaFile);
              // NOTE: A check for existence of deleteDeltaFile is required because we may not have
              // deletes for the bucket being taken into consideration for this split processing.
              if (length != -1 && fs.exists(deleteDeltaFile)) {
                /**
                 * todo: we have OrcSplit.orcTail so we should be able to get stats from there
                 */
                Reader deleteDeltaReader = OrcFile.createReader(deleteDeltaFile,
                    OrcFile.readerOptions(conf).maxLength(length));
                if (deleteDeltaReader.getNumberOfRows() <= 0) {
                  continue; // just a safe check to ensure that we are not reading empty delete files.
                }
                totalDeleteEventCount += deleteDeltaReader.getNumberOfRows();
                DeleteReaderValue deleteReaderValue = new DeleteReaderValue(deleteDeltaReader,
                    deleteDeltaFile, readerOptions, bucket, validWriteIdList, isBucketedTable, conf,
                    keyInterval, orcSplit);
                DeleteRecordKey deleteRecordKey = new DeleteRecordKey();
                if (deleteReaderValue.next(deleteRecordKey)) {
                  sortMerger.put(deleteRecordKey, deleteReaderValue);
                } else {
                  deleteReaderValue.close();
                }
              }
            }
          }
          readAllDeleteEventsFromDeleteDeltas();
          LOG.debug("Number of delete events(limit, actual)=({},{})",
              totalDeleteEventCount, size());
        }
        isEmpty = compressedOwids == null || rowIds == null;
      } catch(IOException|DeleteEventsOverflowMemoryException e) {
        close(); // close any open readers, if there was some exception during initialization.
        throw e; // rethrow the exception so that the caller can handle.
      }
    }
    private void checkSize(int index) throws DeleteEventsOverflowMemoryException {
      if(index > maxEventsInMemory) {
        //check to prevent OOM errors
        LOG.info("Total number of delete events exceeds the maximum number of "
            + "delete events that can be loaded into memory for " + orcSplit
            + ". The max limit is currently set at " + maxEventsInMemory
            + " and can be changed by setting the Hive config variable "
            + ConfVars.HIVE_TRANSACTIONAL_NUM_EVENTS_IN_MEMORY.varname);
        throw new DeleteEventsOverflowMemoryException();
      }
      if(index < rowIds.length) {
        return;
      }
      int newLength = rowIds.length + 1000000;
      if(rowIds.length <= 1000000) {
        //double small arrays; increase by 1M large arrays
        newLength = rowIds.length * 2;
      }
      rowIds = Arrays.copyOf(rowIds, newLength);
    }
    /**
     * This is not done quite right.  The intent of {@link CompressedOwid} is a hedge against
     * "delete from T" that generates a huge number of delete events possibly even 2G - max array
     * size.  (assuming no one txn inserts > 2G rows (in a bucket)).  As implemented, the algorithm
     * first loads all data into one array owid[] and rowIds[] which defeats the purpose.
     * In practice we should be filtering delete evens by min/max ROW_ID from the split.  The later
     * is also not yet implemented: HIVE-16812.
     */
    private void readAllDeleteEventsFromDeleteDeltas()
        throws IOException, DeleteEventsOverflowMemoryException {
      if (sortMerger == null || sortMerger.isEmpty()) {
        return; // trivial case, nothing to read.
      }

      // Initialize the rowId array when we have some delete events.
      rowIds = new long[testMode ? 1 : 10000];

      int index = 0;
      // We compress the owids into CompressedOwid data structure that records
      // the fromIndex(inclusive) and toIndex(exclusive) for each unique owid.
      List<CompressedOwid> compressedOwids = new ArrayList<>();
      CompressedOwid lastCo = null;
      while (!sortMerger.isEmpty()) {
        // The sortMerger is a heap data structure that stores a pair of
        // (deleteRecordKey, deleteReaderValue) at each node and is ordered by deleteRecordKey.
        // The deleteReaderValue is the actual wrapper class that has the reference to the
        // underlying delta file that is being read, and its corresponding deleteRecordKey
        // is the smallest record id for that file. In each iteration of this loop, we extract(poll)
        // the minimum deleteRecordKey pair. Once we have processed that deleteRecordKey, we
        // advance the pointer for the corresponding deleteReaderValue. If the underlying file
        // itself has no more records, then we remove that pair from the heap, or else we
        // add the updated pair back to the heap.
        Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
        DeleteRecordKey deleteRecordKey = entry.getKey();
        DeleteReaderValue deleteReaderValue = entry.getValue();
        long owid = deleteRecordKey.originalWriteId;
        int bp = deleteRecordKey.bucketProperty;
        checkSize(index);
        rowIds[index] = deleteRecordKey.rowId;
        if (lastCo == null || lastCo.originalWriteId != owid || lastCo.bucketProperty != bp) {
          if (lastCo != null) {
            lastCo.toIndex = index; // Finalize the previous record.
          }
          lastCo = new CompressedOwid(owid, bp, index, -1);
          compressedOwids.add(lastCo);
        }
        ++index;
        if (deleteReaderValue.next(deleteRecordKey)) {
          sortMerger.put(deleteRecordKey, deleteReaderValue);
        } else {
          deleteReaderValue.close(); // Exhausted reading all records, close the reader.
        }
      }
      if (lastCo != null) {
        lastCo.toIndex = index; // Finalize the last record.
        lastCo = null;
      }
      if (rowIds.length > index) {
        rowIds = Arrays.copyOf(rowIds, index);
      }

      this.compressedOwids = compressedOwids.toArray(new CompressedOwid[compressedOwids.size()]);
    }


    private boolean isDeleted(long owid, int bucketProperty, long rowId) {
      if (compressedOwids == null || rowIds == null) {
        return false;
      }
      // To find if a given (owid, rowId) pair is deleted or not, we perform
      // two binary searches at most. The first binary search is on the
      // compressed owids. If a match is found, only then we do the next
      // binary search in the larger rowId vector between the given toIndex & fromIndex.

      // Check if owid is outside the range of all owids present.
      if (owid < compressedOwids[0].originalWriteId
          || owid > compressedOwids[compressedOwids.length - 1].originalWriteId) {
        return false;
      }
      // Create a dummy key for searching the owid/bucket in the compressed owid ranges.
      CompressedOwid key = new CompressedOwid(owid, bucketProperty, -1, -1);
      int pos = Arrays.binarySearch(compressedOwids, key);
      if (pos >= 0) {
        // Owid with the given value found! Searching now for rowId...
        key = compressedOwids[pos]; // Retrieve the actual CompressedOwid that matched.
        // Check if rowId is outside the range of all rowIds present for this owid.
        if (rowId < rowIds[key.fromIndex]
            || rowId > rowIds[key.toIndex - 1]) {
          return false;
        }
        if (Arrays.binarySearch(rowIds, key.fromIndex, key.toIndex, rowId) >= 0) {
          return true; // rowId also found!
        }
      }
      return false;
    }
    /**
     * @return how many delete events are actually loaded
     */
    int size() {
      return rowIds == null ? 0 : rowIds.length;
    }
    @Override
    public boolean isEmpty() {
      if(isEmpty == null) {
        throw new IllegalStateException("Not yet initialized");
      }
      return isEmpty;
    }
    @Override
    public void findDeletedRecords(ColumnVector[] cols, int size, BitSet selectedBitSet) {
      if (rowIds == null || compressedOwids == null) {
        return;
      }
      // Iterate through the batch and for each (owid, rowid) in the batch
      // check if it is deleted or not.

      long[] originalWriteIdVector =
          cols[OrcRecordUpdater.ORIGINAL_WRITEID].isRepeating ? null
              : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector;
      long repeatedOriginalWriteId = (originalWriteIdVector != null) ? -1
          : ((LongColumnVector) cols[OrcRecordUpdater.ORIGINAL_WRITEID]).vector[0];

      long[] bucketProperties =
        cols[OrcRecordUpdater.BUCKET].isRepeating ? null
          : ((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector;
      int repeatedBucketProperty = (bucketProperties != null) ? -1
        : (int)((LongColumnVector) cols[OrcRecordUpdater.BUCKET]).vector[0];

      long[] rowIdVector =
          ((LongColumnVector) cols[OrcRecordUpdater.ROW_ID]).vector;

      for (int setBitIndex = selectedBitSet.nextSetBit(0);
          setBitIndex >= 0;
          setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
        long owid = originalWriteIdVector != null ? originalWriteIdVector[setBitIndex]
                                                    : repeatedOriginalWriteId ;
        int bucketProperty = bucketProperties != null ? (int)bucketProperties[setBitIndex]
          : repeatedBucketProperty;
        long rowId = rowIdVector[setBitIndex];
        if (isDeleted(owid, bucketProperty, rowId)) {
          selectedBitSet.clear(setBitIndex);
        }
     }
    }

    @Override
    public void close() throws IOException {
      // ColumnizedDeleteEventRegistry reads all the delete events into memory during initialization
      // and it closes the delete event readers after it. If an exception gets thrown during
      // initialization, we may have to close any readers that are still left open.
      while (!sortMerger.isEmpty()) {
        Entry<DeleteRecordKey, DeleteReaderValue> entry = sortMerger.pollFirstEntry();
        entry.getValue().close(); // close the reader for this entry
      }
    }
  }

  static class DeleteEventsOverflowMemoryException extends Exception {
    private static final long serialVersionUID = 1L;
  }
  @VisibleForTesting
  OrcRawRecordMerger.KeyInterval getKeyInterval() {
    return keyInterval;
  }
  @VisibleForTesting
  SearchArgument getDeleteEventSarg() {
     return deleteEventSarg;
  }
}
