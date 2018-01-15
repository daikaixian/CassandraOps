package org.water.devops.jtool4cassandra;

import org.junit.Assert;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import java.util.List;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一致性校验的基本思路是，比较同一个cluster的两个dc中，每个keyspace的每个table的数据是否一致。
 * 理论上，同样适用于比较两个不同cluster的数据。
 *
 * check的逻辑比较粗暴，按照token(primary_key)的值对表中的数据进行排序，然后分页读取，toString之后再两两比对。
 * 单元测试运行失败的情况大概可分为两种
 * 1. 两个dc的数据确实不相同，比对失败，所以单测执行失败。
 * 2. 在check的过程中，业务还在继续往cassandra中写入数据。虽然两个dc之间会同步数据，但是同步数据也是会有延迟的。如果往一个dc中写入，
 *    而且根据token(primary_key)计算出来的值，正好是脚本正在比对的区域。则有可能导致单测失败。碰到这种情况，不要断定为数据不一致，
 *    可以记录下最后的token(primary_key),然后从这里为起点，重新run脚本。只有在多次运行失败，且每次都卡在同一个位置的时候，可以认为
 *    两个dc的数据一致性是有问题的。
 *
 * 一致性验证的逻辑是逐表check.所以如果想提升check的速度。可以针对每一张表，写一个test method。然后利用mvn的surefire插件，设置并发参数
 * 即可以提升check效率。当然，大并发的query需要考虑下Cassandra集群的I/O承受能力。
 *
 */
public class CassandraReplicationConsistencyCheckTest extends BaseTest{

    /**
     * 把debug和info信息写入到两个不同的日志文件，方便观察脚本执行的进度。
     */
    private static Logger logger = LoggerFactory.getLogger("checker_logger");
    private static Logger debugger = LoggerFactory.getLogger("checker_debugger");


    @Test
    public void testCheckDemo() throws NoSuchAlgorithmException {
        /**
         * 表名: demo_table
         * 线上数据count：9392853 count 加10000的buffer，因为在执行一致性check的过程中，还会有数据写入。
         * 获取该表的count有两种方式：
         *   1.如果表数据不大，可以直接select count(*) from table.
         *   2.如果表数据很大，上面的操作会超时，这个时候需要参考本项目中的另一个工具，即："计算大表的count"。
         *
         */
        int count = 9402853;
        String keyspace = "demo_keyspace";
        String tableName = "demo_table";
        /**
         * demo_table的表结构：

         CREATE TABLE demo_keyspace.demo_table (
         user_id bigint,
         filed1 text,
         filed2 text,
         created bigint,
         updated bigint,
         PRIMARY KEY (user_id, filed1, filed2)
         ) WITH CLUSTERING ORDER BY (filed1 ASC, filed2 ASC)
         AND bloom_filter_fp_chance = 0.01
         AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
         AND comment = ''
         AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
         AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
         AND crc_check_chance = 1.0
         AND dclocal_read_repair_chance = 0.1
         AND default_time_to_live = 0
         AND gc_grace_seconds = 864000
         AND max_index_interval = 2048
         AND memtable_flush_period_in_ms = 0
         AND min_index_interval = 128
         AND read_repair_chance = 0.0
         AND speculative_retry = '99PERCENTILE';
         *
         *
         *
         */
        String selectCql = "select token(user_id), user_id, filed1, filed2, created, updated " +
                "from  demo_table where token(user_id) > %d limit %d ALLOW FILTERING";
        checkMethod(keyspace,tableName, selectCql, count);
    }

    private void checkMethod(String keyspace,String tableName, String selectCql, int count){
        int currentSum = 0;
        int limit = 1000;  // 分页的size. 可以根据自己的需要调整。

        /**
         * 设置query的一致性等级，整体思路是每个session只从一个dc中读取数据。不可以跨dc查询。
         */
        DCAwareRoundRobinPolicy.Builder policyBuilder = DCAwareRoundRobinPolicy.builder();
        QueryOptions qo = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM); //一致性等级非常重要。必须是Local_xx

        /**
         * 从dc1中读数据。
         */
        Cluster cluster1 = Cluster.builder()
                .addContactPoints("10.10.10.11","10.10.10.12","10.10.10.13") // cassandra node host。按dc来分组。数量最好是每个dc有几个就填几个。
                .withPort(9042)  //端口
                .withCredentials("username", "password")
                .withLoadBalancingPolicy(policyBuilder.withLocalDc("dc1").build())
                .withQueryOptions(qo)
                .build();

        Session session1 = cluster1.connect(keyspace);


        /**
         * 从dc2中读数据。
         */
        Cluster cluster2 = Cluster.builder()
                .addContactPoints("10.10.20.11","10.10.20.12","10.10.20.13") // cassandra node host。按dc来分组。数量最好是每个dc有几个就填几个。
                .withPort(9042) //端口
                .withCredentials("username", "password")
                .withLoadBalancingPolicy(policyBuilder.withLocalDc("dc2").build())
                .withQueryOptions(qo)
                .build();

        Session session2 = cluster2.connect(keyspace);

        // 这里的lastId 初始的时候是从最小值开始计算。如果出现中途挂掉的情况，可从最后挂掉的token(primary_key)开始，重新跑。
        long lastId = Long.MIN_VALUE;
        // 分页抓取，比对。
        while (currentSum < count) {
            String currentCql = String.format(selectCql, lastId, limit);

            ResultSet rs1 = null;
            try {
                rs1 = session1.execute(currentCql);
            } catch (UnavailableException unavailableException) {
                System.out.println("*************出现这个异常说明这个dc查不到对应的数据**********************");
            }

            ResultSet rs2 = null;
            try {
                rs2 = session2.execute(currentCql);
            } catch (UnavailableException unavailableException) {
                System.out.println("*************出现这个异常说明这个dc查不到对应的数据**********************");
            }

            List<Row> rs1List = new ArrayList<>();
            List<Row> rs2List = new ArrayList<>();
            if (rs1 != null && rs2 != null) {

                rs1List = rs1.all();
                rs2List = rs2.all();
                Assert.assertEquals(rs1List.size(), rs2List.size());
                StringBuffer buff1 = new StringBuffer("");
                StringBuffer buff2 = new StringBuffer("");
                for (Row row : rs1List) {
                    buff1.append(row.toString());
                }
                for (Row row : rs2List) {
                    buff2.append(row.toString());
                }

                String original1 = buff1.toString();
                String original2 = buff2.toString();

                /**
                 * 比对逻辑在这里。
                 * 一开始考虑转换成MD5再比对，后来想想其实没什么必要。原始字符串的值相同，就可以断定为两者数据是一致的
                 */
                Assert.assertTrue(original1.equals(original2));
            }
            // 该页数据比对正常之后
            if (rs1List.isEmpty() && rs2List.isEmpty()) {
                /**
                 * 可能查询失败了,或者就是没数据，要跳过
                 */
                currentSum += limit;
                debugger.info(keyspace +":" +tableName +":current sum:"+currentSum);
            } else {
                // 一切正常，开始比对下一页。
                lastId = (long) rs1List.get(rs1List.size() - 1).getToken(0).getValue();
                currentSum += limit;
                debugger.info(keyspace +":" +tableName +":current sum:"+currentSum);
                debugger.info("debug,token(primary_key)到了哪： "+ lastId);
            }
        }
        /**
         *最后运行完之后，记录token(primary_key)的最后值。相当于记录了最后一行
         */
        logger.info(keyspace +":" +tableName + ":lastId = " +lastId);
    }






}
