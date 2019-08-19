AS of now this config seems to determine if we are using the auto scaling mode  `org.apache.hadoop.hive.conf.HiveConf.ConfVars#LLAP_DAEMON_SERVICE_HOSTS_ENABLE_COMPUTE_GROUPS`

True means we are using an extra level to group llap nodes and am(s) by group number.
True implies that llap daemons  will register under @compute/computeGroup as oppose to @compute when false.
Therefore now HS2 has to list this directory using a TreeNode and adjust to the increase/decrease of groups.

First pass is done by this patch in TEZ 
https://github.infra.cloudera.com/CDH/tez/commit/0eb45d59cbbfa95e53453a12da1b0b348a301574
DWX-10: EDWS Autoscaling

Second one in Hive repo 
DWX-10: EDWS Autoscaling
38ebdfc5c740ef0829c6b081c1604b412a016fc4

The goal here is to add to HS2 the mechanism that can be used to list nodes

# Implementation notes 
Most of the logic of llap znodes registry and registration is in this class org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl

The above class extendes the base class org.apache.hadoop.hive.registry.impl.ZkRegistryBase

ZkRegistryBase contains most of the base function to write stuff into Zk and read it back, the subclasses of this, will control the paylod to be written and the logic around it.


`org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService#getClient`
is the entry method to provide the llap registry service. our main implementation is zk based.
as side note one other implementation is fixed on exact hostnames

## How this works 
`org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl#LlapZookeeperRegistryImpl`
is used by the llap nodes to register them self, and by the AM eg query coordinator to discover llap nodes and by HS2 to discover llap nodes as well.

AM and HS2 access the registry via `org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService#getClient`


# Questions 

what is `org.apache.hadoop.hive.registry.impl.TezAmRegistryImpl` and how it works with 

```shell
tez-api/src/main/java/org/apache/tez/client/registry/zookeeper/ZkAMRegistryClient.java
tez-api/src/main/java/org/apache/tez/client/registry/AMRegistry.java
tez-api/src/main/java/org/apache/tez/client/registry/AMRecord.java
```

What this class is used for `org.apache.hadoop.hive.ql.exec.tez.TezSessionPool#TezSessionPool`



```
get /llap-unsecure/user-sbouguerra/sbouguerra-llap0/workers/slot-0000000000
5626f11b-e770-460b-955a-e82a97d0f922
cZxid = 0x800153ca8
ctime = Tue Jul 09 19:10:40 EDT 2019
mZxid = 0x800153ca8
mtime = Tue Jul 09 19:10:40 EDT 2019
pZxid = 0x800153ca8
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x3693b6caae904f5
dataLength = 36
numChildren = 0
get /llap-unsecure/user-sbouguerra/sbouguerra-llap0/workers/worker-0000000702
{"type":"JSONServiceRecord","external":[{"api":"services","addressType":"uri","protocolType":"webui","addresses":[{"uri":"http://cn116-10.l42scl.hortonworks.com:15002"}]}],"internal":[{"api":"llap","addressType":"host/port","protocolType":"hadoop/IPC","addresses":[{"host":"cn116-10.l42scl.hortonworks.com","port":"36567"}]},{"api":"llapmng","addressType":"host/port","protocolType":"hadoop/IPC","addresses":[{"host":"cn116-10.l42scl.hortonworks.com","port":"15004"}]},{"api":"shuffle","addressType":"host/port","protocolType":"tcp","addresses":[{"host":"cn116-10.l42scl.hortonworks.com","port":"15551"}]},{"api":"llapoutputformat","addressType":"host/port","protocolType":"hadoop/IPC","addresses":[{"host":"cn116-10.l42scl.hortonworks.com","port":"15003"}]}],"hive.llap.daemon.container.id":"container_e02_1520459437616_20580_01_000012","hive.llap.client.consistent.splits":"true","hive.llap.daemon.yarn.container.mb":"180000","hive.llap.io.allocator.mmap":"false","hive.llap.io.memory.size":"33554432000","hive.llap.task.scheduler.locality.delay":"-1","hive.llap.management.rpc.port":"15004","hive.llap.daemon.task.scheduler.enabled.wait.queue.size":"10","hive.llap.daemon.rpc.port":"36567","hive.llap.daemon.nm.address":"cn116-10.l42scl.hortonworks.com:45454","llap.daemon.metrics.sessionid":"09ef6340-aa6a-486b-af53-9d1bc147d21b","hive.llap.daemon.allow.permanent.fns":"false","hive.llap.io.enabled":"true","hive.llap.daemon.num.enabled.executors":"32","registry.unique.id":"8f21df76-066f-4a67-90d1-c4eb46235a3f","hive.llap.daemon.web.port":"15002","hive.llap.object.cache.enabled":"true","hive.llap.execution.mode":"only","hive.llap.daemon.yarn.shuffle.port":"15551","hive.llap.daemon.output.service.port":"15003","hive.llap.daemon.task.scheduler.wait.queue.size":"10","hive.llap.daemon.memory.per.instance.mb":"128000","hive.llap.daemon.work.dirs":"${yarn.nodemanager.local-dirs}","hive.llap.io.threadpool.size":"32","hive.llap.daemon.service.hosts":"@sbouguerra-llap0","hive.llap.auto.allow.uber":"false","hive.llap.daemon.task.scheduler.enable.preemption":"true","hive.llap.daemon.num.executors":"32"}
cZxid = 0x800153cc2
ctime = Tue Jul 09 19:10:54 EDT 2019
mZxid = 0x800153cc2
mtime = Tue Jul 09 19:10:54 EDT 2019
pZxid = 0x800153cc2
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x16b95b8913c0029
dataLength = 2109
numChildren = 0

```