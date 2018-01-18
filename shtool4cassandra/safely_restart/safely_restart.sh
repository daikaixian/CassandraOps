# 直接kill显得过于粗暴,所以在kill之前新把node对外的一些联系斩断
#先进到你安装cassandra的目录
cd $CASSANDRA_INSTALL_PATH
#Disable Gossip
bin/nodetool disablegossip
# Disable thrift
bin/nodetool disablethrift
# converts Memtables into immutable SSTables, emptying Commit Log
bin/nodetool drain

#以上三步执行完毕之后，就可以做后续的stop或者restart操作了。

# stop.  先找到pid ,然后kill.
ps auwx | grep cassandra
sudo kill pid

# restart  先kill 后启动
bin/cassandra

