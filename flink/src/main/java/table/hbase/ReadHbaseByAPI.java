package table.hbase;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.HBase;
import table.dynamic.UserVo;

/**
 * @Author: wangsen
 * @Date: 2020/4/10 19:39
 * @Description:
 **/
public class ReadHbaseByAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);



        tableEnvironment.connect(
                new HBase()
                        .version("1.4.3")                      // required: currently only support "1.4.3"
                        .tableName("test")         // required: HBase table name
                        .zookeeperQuorum("master:2181")     // required: HBase Zookeeper quorum configuration
                        .zookeeperNodeParent("/hbase")          // optional: the root dir in Zookeeper for HBase cluster.
                        // The default value is "/hbase".
                        .writeBufferFlushMaxSize("10mb")       // optional: writing option, determines how many size in memory of buffered
                        // rows to insert per round trip. This can help performance on writing to JDBC
                        // database. The default value is "2mb".
                        .writeBufferFlushMaxRows(1000)         // optional: writing option, determines how many rows to insert per round trip.
                        // This can help performance on writing to JDBC database. No default value,
                        // i.e. the default flushing is not depends on the number of buffered rows.
                        .writeBufferFlushInterval("2s")        // optional: writing option, sets a flush interval flushing buffered requesting
                // if the interval passes, in milliseconds. Default value is "0s", which means
                // no asynchronous flush thread will be scheduled.
        );

        String query = "SELECT user,SUM(age) as countage FROM test group by user";
        Table table = tableEnvironment.sqlQuery(query);

        DataStream<Tuple2<Boolean, UserVo>> rowDataStream = tableEnvironment.toRetractStream(table, UserVo.class);
        rowDataStream.print();
        environment.execute();
    }
}
