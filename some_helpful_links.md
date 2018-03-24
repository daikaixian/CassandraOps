# 日常维护指南

- 修改keyspace之后，无法登陆，或者权限不足，或者日志中出现 UnavailableException: Cannot achieve consistency level LOCAL_ONE等症状。赶紧repair. 参考：https://support.datastax.com/hc/en-us/articles/204436229-Read-and-login-failures-after-updating-keyspace-replication
- 关于如何修改keyspace的replication strategy 以及一些注意事项：https://docs.datastax.com/en/cassandra/3.0/cassandra/operations/opsChangeKSStrategy.html
- 辅助理解Cassandra 可调节一致性的特性的工具：https://www.ecyrd.com/cassandracalculator/
- 区分Replication Factory和Consistency Level：https://docs.apigee.com/private-cloud/latest/about-cassandra-replication-factor-and-consistency-level
- cassandra 对事务的支持，比较弱：https://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlLtwtTransactions.html
- cassandra 用pssh生成全局快照。https://shiv4nsh.wordpress.com/2016/12/18/cassandra-global-snapshot-taking-dump-of-a-keyspace-for-whole-cluster/
- cassandra 节点比较科学的restart 方法：https://github.com/xluffy/til/issues/20
- cassandra count 比较大的表。select count(*) 容易超时，可以试试这个奇淫巧技：https://stackoverflow.com/questions/36744210/select-count-runs-into-timeout-issues-in-cassandra/36745042#36745042
- Cassandra 节点状态的一些监控指令：https://stackoverflow.com/questions/26285616/cassandra-multi-datacenter-data-sync-lag
- cassandra repair状态监控：https://stackoverflow.com/questions/25064717/how-do-i-know-if-nodetool-repair-is-finished
- Cassandra数据一致性验证脚本 ，参考：http://gitlab.mogujie.org/kaishui/mymoguboot/blob/online_feature/cassandra_consistency_check/web/src/test/java/org/kaishui/moguboot/configuration/ca/dao/PlanetTest.java
- Cassandra 替换异常节点：https://docs.datastax.com/en/cassandra/3.0/cassandra/operations/opsReplaceLiveNode.html


- 持续更新

