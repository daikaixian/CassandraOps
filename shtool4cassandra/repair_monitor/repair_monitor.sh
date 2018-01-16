#当前目录下 要有cassandra_hosts文件，里面记录了远程登录到目标服务器的基本信息。用户名，host,端口等
#比如 ssh_username@10.10.10.11:22。 每行一个主机。一般cassandra会有多个节点，所以每个节点都要配置进去,就可以一次性看到所有节点当前repair的进度

pssh -h cassandra_hosts -l ssh_username -A -i "$CASSANDRA_INSTALL_PATH/bin/nodetool compactionstats"



