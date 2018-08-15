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

package org.apache.hadoop.hive.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Kafka Range trimmer, takes a full kafka scan and prune the scan based on a filter expression
 * it is a Best effort trimmer and it can not replace the filter it self, filtration still takes place in Hive executor.
 */
public class KafkaScanTrimmer {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaScanTrimmer.class);
  private final Map<TopicPartition, KafkaPullerInputSplit> fullHouse;
  private final KafkaConsumer kafkaConsumer;

  /**
   * @param fullHouse     initial full scan to be pruned, this is a map of Topic partition to input split.
   * @param kafkaConsumer kafka consumer used to pull offsets for time filter if needed
   */
  public KafkaScanTrimmer(Map<TopicPartition, KafkaPullerInputSplit> fullHouse, KafkaConsumer kafkaConsumer) {
    this.fullHouse = fullHouse;
    this.kafkaConsumer = kafkaConsumer;
  }

  /**
   * This might block due to calls like.
   * org.apache.kafka.clients.consumer.KafkaConsumer#offsetsForTimes(java.util.Map)
   *
   * @param filterExpression filter expression to be used for pruning scan
   *
   * @return tiny house of of the full house based on filter expression
   */
  public Map<TopicPartition, KafkaPullerInputSplit> computeOptimizedScan(ExprNodeGenericFuncDesc filterExpression) {
    Map<TopicPartition, KafkaPullerInputSplit> optimizedScan = parse(filterExpression);

    if (LOG.isDebugEnabled()) {
      if (optimizedScan != null) {
        LOG.debug("Optimized scan:");
        optimizedScan.forEach((tp, input) -> LOG.info(
            "Topic-[{}] Partition-[{}] - Split startOffset [{}] :-> endOffset [{}]",
            tp.topic(),
            tp.partition(),
            input.getStartOffset(),
            input.getEndOffset()));
      } else {
        LOG.debug("No optimization thus using full scan ");
        fullHouse.forEach((tp, input) -> LOG.info(
            "Topic-[{}] Partition-[{}] - Split startOffset [{}] :-> endOffset [{}]",
            tp.topic(),
            tp.partition(),
            input.getStartOffset(),
            input.getEndOffset()));
      }
    }
    return optimizedScan == null ? fullHouse : optimizedScan;
  }

  /**
   * @param expression filter to parse and trim the full scan
   *
   * @return Map of optimized kafka range scans or null if it is impossible to optimize.
   */
  @Nullable private Map<TopicPartition, KafkaPullerInputSplit> parse(ExprNodeDesc expression) {
    if (expression.getClass() != ExprNodeGenericFuncDesc.class) {
      return null;
    }
    // get the kind of expression
    ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) expression;
    Class<?> op = expr.getGenericUDF().getClass();

    // handle the logical operators
    if (FunctionRegistry.isOpOr(expr)) {
      return pushOrOp(expr);
    }
    if (FunctionRegistry.isOpAnd(expr)) {
      return pushAndOp(expr);
    }

    if (op == GenericUDFOPGreaterThan.class) {
      return pushLeaf(expr, PredicateLeaf.Operator.LESS_THAN_EQUALS, true);
    } else if (op == GenericUDFOPEqualOrGreaterThan.class) {
      return pushLeaf(expr, PredicateLeaf.Operator.LESS_THAN, true);
    } else if (op == GenericUDFOPLessThan.class) {
      return pushLeaf(expr, PredicateLeaf.Operator.LESS_THAN, false);
    } else if (op == GenericUDFOPEqualOrLessThan.class) {
      return pushLeaf(expr, PredicateLeaf.Operator.LESS_THAN_EQUALS, false);
    } else if (op == GenericUDFOPEqual.class) {
      return pushLeaf(expr, PredicateLeaf.Operator.EQUALS, false);
      // otherwise, we didn't understand it, so bailout
    } else {
      return null;
    }
  }

  /**
   * @param expr     leaf node to push
   * @param operator operator
   * @param negation true if it is a negation, this is used to represent:
   *                 GenericUDFOPGreaterThan and GenericUDFOPEqualOrGreaterThan
   *                 using PredicateLeaf.Operator.LESS_THAN and PredicateLeaf.Operator.LESS_THAN_EQUALS
   *
   * @return leaf scan or null if can not figure out push down
   */
  @Nullable private Map<TopicPartition, KafkaPullerInputSplit> pushLeaf(ExprNodeGenericFuncDesc expr,
      PredicateLeaf.Operator operator,
      boolean negation) {
    if (expr.getChildren().size() != 2) {
      return null;
    }
    GenericUDF genericUDF = expr.getGenericUDF();
    if (!(genericUDF instanceof GenericUDFBaseCompare)) {
      return null;
    }
    ExprNodeDesc expr1 = expr.getChildren().get(0);
    ExprNodeDesc expr2 = expr.getChildren().get(1);
    // We may need to peel off the GenericUDFBridge that is added by CBO or user
    if (expr1.getTypeInfo().equals(expr2.getTypeInfo())) {
      expr1 = getColumnExpr(expr1);
      expr2 = getColumnExpr(expr2);
    }

    ExprNodeDesc[] extracted = ExprNodeDescUtils.extractComparePair(expr1, expr2);
    if (extracted == null || (extracted.length > 2)) {
      return null;
    }

    ExprNodeColumnDesc columnDesc;
    ExprNodeConstantDesc constantDesc;
    final boolean flip;

    if (extracted[0] instanceof ExprNodeColumnDesc) {
      columnDesc = (ExprNodeColumnDesc) extracted[0];
      constantDesc = (ExprNodeConstantDesc) extracted[1];
      flip = false;

    } else {
      flip = true;
      columnDesc = (ExprNodeColumnDesc) extracted[1];
      constantDesc = (ExprNodeConstantDesc) extracted[0];
    }

    if (columnDesc.getColumn().equals(KafkaStorageHandler.PARTITION_COLUMN)) {
      return buildScanFromPartitionPredicate(fullHouse,
          operator,
          ((Number) constantDesc.getValue()).intValue(),
          flip,
          negation);

    }
    if (columnDesc.getColumn().equals(KafkaStorageHandler.OFFSET_COLUMN)) {
      return buildScanFromOffsetPredicate(fullHouse,
          operator,
          ((Number) constantDesc.getValue()).longValue(),
          flip,
          negation);
    }

    if (columnDesc.getColumn().equals(KafkaStorageHandler.TIMESTAMP_COLUMN)) {
      long timestamp = ((Number) constantDesc.getValue()).longValue();
      return buildScanForTimesPredicate(fullHouse, operator, timestamp, flip, negation, kafkaConsumer);
    }
    return null;
  }

  /**
   * Trim kafka scan using a leaf binary predicate on partition column.
   *
   * @param fullScan       kafka full scan to be optimized
   * @param operator       predicate operator, equal, lessThan or lessThanEqual
   * @param partitionConst partition constant value
   * @param flip           true if the position of column and constant is flipped by default assuming column OP constant
   * @param negation       true if the expression is a negation of the original expression
   *
   * @return filtered kafka scan
   */
  @VisibleForTesting protected static Map<TopicPartition, KafkaPullerInputSplit> buildScanFromPartitionPredicate(
      Map<TopicPartition, KafkaPullerInputSplit> fullScan,
      PredicateLeaf.Operator operator,
      int partitionConst,
      boolean flip,
      boolean negation) {
    final Predicate<TopicPartition> predicate;
    final Predicate<TopicPartition> intermediatePredicate;
    switch (operator) {
    case EQUALS:
      predicate = topicPartition -> topicPartition != null && topicPartition.partition() == partitionConst;
      break;
    case LESS_THAN:
      intermediatePredicate =
          flip ?
              topicPartition -> topicPartition != null && partitionConst < topicPartition.partition() :
              topicPartition -> topicPartition != null && topicPartition.partition() < partitionConst;

      predicate = negation ? intermediatePredicate.negate() : intermediatePredicate;
      break;
    case LESS_THAN_EQUALS:
      intermediatePredicate =
          flip ?
              topicPartition -> topicPartition != null && partitionConst <= topicPartition.partition() :
              topicPartition -> topicPartition != null && topicPartition.partition() <= partitionConst;

      predicate = negation ? intermediatePredicate.negate() : intermediatePredicate;
      break;
    default:
      //Default to select * for unknown cases
      predicate = topicPartition -> true;
    }

    ImmutableMap.Builder<TopicPartition, KafkaPullerInputSplit> builder = ImmutableMap.builder();
    // Filter full scan based on predicate
    fullScan.entrySet()
        .stream()
        .filter(entry -> predicate.test(entry.getKey()))
        .forEach(entry -> builder.put(entry.getKey(), entry.getValue().clone()));
    return builder.build();
  }

  /**
   * @param fullScan    full kafka scan to be pruned
   * @param operator    operator kind
   * @param offsetConst offset constant value
   * @param flip        true if position of constant and column were flipped by default assuming COLUMN OP CONSTANT
   * @param negation    true if the expression is a negation of the original expression
   *
   * @return optimized kafka scan
   */
  @VisibleForTesting protected static Map<TopicPartition, KafkaPullerInputSplit> buildScanFromOffsetPredicate(
      Map<TopicPartition, KafkaPullerInputSplit> fullScan,
      PredicateLeaf.Operator operator,
      long offsetConst,
      boolean flip,
      boolean negation) {
    final boolean isEndBound;
    final long startOffset;
    final long endOffset;

    if (flip == negation) {
      isEndBound = true;
    } else {
      isEndBound = false;
    }
    switch (operator) {
    case LESS_THAN_EQUALS:
      if (isEndBound) {
        startOffset = -1;
        endOffset = negation ? offsetConst : offsetConst + 1;
      } else {
        endOffset = -1;
        startOffset = negation ? offsetConst + 1 : offsetConst;
      }
      break;
    case EQUALS:
      startOffset = offsetConst;
      endOffset = offsetConst + 1;
      break;
    case LESS_THAN:
      if (isEndBound) {
        endOffset = negation ? offsetConst + 1 : offsetConst;
        startOffset = -1;
      } else {
        endOffset = -1;
        startOffset = negation ? offsetConst : offsetConst + 1;
      }
      break;
    default:
      // default to select *
      startOffset = -1;
      endOffset = -1;
    }

    final Map<TopicPartition, KafkaPullerInputSplit> newScan = new HashMap<>();

    fullScan.forEach((tp, existingInputSplit) -> {
      final KafkaPullerInputSplit newInputSplit;
      if (startOffset != -1 && endOffset == -1) {
        newInputSplit = new KafkaPullerInputSplit(tp.topic(),
            tp.partition(),
            // @TODO make sure that this is okay
            //if the user ask for start offset > max offset will replace with last offset
            Math.min(startOffset, existingInputSplit.getEndOffset()),
            existingInputSplit.getEndOffset(),
            existingInputSplit.getPath());
      } else if (endOffset != -1 && startOffset == -1) {
        newInputSplit = new KafkaPullerInputSplit(tp.topic(), tp.partition(), existingInputSplit.getStartOffset(),
            //@TODO check this, if user ask for non existing end offset ignore it and position head on start
            Math.max(endOffset, existingInputSplit.getStartOffset()), existingInputSplit.getPath());
      } else if (endOffset == startOffset + 1) {
        if (startOffset < existingInputSplit.getStartOffset() || startOffset >= existingInputSplit.getEndOffset()) {
          newInputSplit = new KafkaPullerInputSplit(tp.topic(), tp.partition(),
              //@TODO check this with team if we have ask for offset out of range what to do ?
              // here am seeking to last offset
              existingInputSplit.getEndOffset(), existingInputSplit.getEndOffset(), existingInputSplit.getPath());
        } else {
          newInputSplit =
              new KafkaPullerInputSplit(tp.topic(),
                  tp.partition(),
                  startOffset,
                  endOffset,
                  existingInputSplit.getPath());
        }

      } else {
        newInputSplit =
            new KafkaPullerInputSplit(tp.topic(),
                tp.partition(),
                existingInputSplit.getStartOffset(),
                existingInputSplit.getEndOffset(),
                existingInputSplit.getPath());
      }

      newScan.put(tp, KafkaPullerInputSplit.intersectRange(newInputSplit, existingInputSplit));
    });

    return newScan;
  }

  @Nullable protected static Map<TopicPartition, KafkaPullerInputSplit> buildScanForTimesPredicate(
      Map<TopicPartition, KafkaPullerInputSplit> fullHouse,
      PredicateLeaf.Operator operator,
      long timestamp,
      boolean flip,
      boolean negation,
      KafkaConsumer consumer) {
    long
        increment =
        (flip && operator == PredicateLeaf.Operator.LESS_THAN
            || negation && operator == PredicateLeaf.Operator.LESS_THAN_EQUALS) ? 1L : 0L;
    // only accepted cases are timestamp_column [ > ; >= ; = ]constant
    if (operator == PredicateLeaf.Operator.EQUALS || flip ^ negation) {
      final Map<TopicPartition, Long> timePartitionsMap = Maps.toMap(fullHouse.keySet(), tp -> timestamp + increment);
      try {
        // Based on Kafka docs
        // NULL will be returned for that partition If the message format version in a partition is before 0.10.0
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = consumer.offsetsForTimes(timePartitionsMap);
        final Map<TopicPartition, KafkaPullerInputSplit> newScan = Maps.toMap(fullHouse.keySet(), tp -> {
          KafkaPullerInputSplit existing = fullHouse.get(tp);
          OffsetAndTimestamp foundOffsetAndTime = offsetAndTimestamp.get(tp);
          //Null in case filter doesn't match or field not existing ie old broker thus return empty scan.
          final long startOffset = foundOffsetAndTime == null ? existing.getEndOffset() : foundOffsetAndTime.offset();
          return new KafkaPullerInputSplit(tp.topic(),
              tp.partition(),
              startOffset,
              existing.getEndOffset(),
              existing.getPath());
        });
        return newScan;
      } catch (Exception e) {
        LOG.error("Error while looking up offsets for time", e);
        //Bailout when can not figure out offsets for times.
        return null;
      }

    }
    return null;
  }

  /**
   * @param expr And expression to be parsed
   *
   * @return either full scan or an optimized sub scan.
   */
  private Map<TopicPartition, KafkaPullerInputSplit> pushAndOp(ExprNodeGenericFuncDesc expr) {
    Map<TopicPartition, KafkaPullerInputSplit> currentScan = new HashMap<>();

    fullHouse.forEach((tp, input) -> currentScan.put(tp, KafkaPullerInputSplit.copyOf(input)));

    for (ExprNodeDesc child : expr.getChildren()) {
      Map<TopicPartition, KafkaPullerInputSplit> scan = parse(child);
      if (scan != null) {
        Set<TopicPartition> currentKeys = ImmutableSet.copyOf(currentScan.keySet());
        currentKeys.stream().forEach(key -> {
          KafkaPullerInputSplit newSplit = scan.get(key);
          KafkaPullerInputSplit oldSplit = currentScan.get(key);
          currentScan.remove(key);
          if (newSplit != null) {
            KafkaPullerInputSplit intersectionSplit = KafkaPullerInputSplit.intersectRange(newSplit, oldSplit);
            if (intersectionSplit != null) {
              currentScan.put(key, intersectionSplit);
            }
          }
        });

      }
    }
    return currentScan;
  }

  @Nullable private Map<TopicPartition, KafkaPullerInputSplit> pushOrOp(ExprNodeGenericFuncDesc expr) {
    final Map<TopicPartition, KafkaPullerInputSplit> currentScan = new HashMap<>();
    for (ExprNodeDesc child : expr.getChildren()) {
      Map<TopicPartition, KafkaPullerInputSplit> scan = parse(child);
      if (scan == null) {
        // if any of the children is unknown bailout
        return null;
      }

      scan.forEach((tp, input) -> {
        KafkaPullerInputSplit existingSplit = currentScan.get(tp);
        currentScan.put(tp, KafkaPullerInputSplit.unionRange(input, existingSplit == null ? input : existingSplit));
      });
    }
    return currentScan;
  }

  private static ExprNodeDesc getColumnExpr(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc) {
      return expr;
    }
    ExprNodeGenericFuncDesc funcDesc = null;
    if (expr instanceof ExprNodeGenericFuncDesc) {
      funcDesc = (ExprNodeGenericFuncDesc) expr;
    }
    if (null == funcDesc) {
      return expr;
    }
    GenericUDF udf = funcDesc.getGenericUDF();
    // check if its a simple cast expression.
    if ((udf instanceof GenericUDFBridge
        || udf instanceof GenericUDFToBinary
        || udf instanceof GenericUDFToChar
        || udf instanceof GenericUDFToVarchar
        || udf instanceof GenericUDFToDecimal
        || udf instanceof GenericUDFToDate
        || udf instanceof GenericUDFToUnixTimeStamp
        || udf instanceof GenericUDFToUtcTimestamp) && funcDesc.getChildren().size() == 1 && funcDesc.getChildren()
        .get(0) instanceof ExprNodeColumnDesc) {
      return expr.getChildren().get(0);
    }
    return expr;
  }

}
