
# 对于小表，可以直接 select count(*) from target_table;
# 但是对于大表，这样肯定会超时的。
# 那么如何获取大表的count了？

cd $CASSANDRA_INSTALL_PATH/bin/
./cqlsh $host $port -u username -p pwd
#登录cqlsh 之后，执行以下操作。
use target_keyspace;
COPY target_table TO '/dev/null';


#更多详情可参考 https://stackoverflow.com/questions/36744210/select-count-runs-into-timeout-issues-in-cassandra/36745042#36745042


