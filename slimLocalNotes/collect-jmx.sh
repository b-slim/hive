#!/usr/bin/env bash

while [[ 1 ]]
do
sleep 5
curl http://ctr-e138-1518143905142-420129-01-000010.hwx.site:15002/jmx \
| grep -e '\"Allocator'  -e '\"Uptime\"' -e'CacheEvictedTotalBytes' \
-e '\"Allocation' -e 'CacheTotalReserved' -e 'EvictionDispatcherMetaDataBytes' -e 'CacheCapacityRemaining\"' \
-e 'CacheCapacityUsed'  -e 'CacheRequestedBytes :' -e 'CachePolicyEvictedBytes' -e 'EvictionDispatcherDataBytes'|\
xargs  echo #>> /grid/1/run-logs/jmx-collection-24.2.txt
done
