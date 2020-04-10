package table.hbase;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import table.dynamic.UserVo;

/**
 * @Author: wangsen
 * @Date: 2020/4/10 16:34
 * @Description:
 **/
public class ReadHbaseByDDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        String createTable = "CREATE TABLE MyUserTable (" +
                "   rowkey string,"+
                "   cf ROW<`user` STRING,age DECIMAL> )" +
                " WITH (\n" +
                "  'connector.type' = 'hbase', -- required: specify this table type is hbase\n" +
                "  \n" +
                "  'connector.version' = '1.4.3',          -- required: valid connector versions are \"1.4.3\"\n" +
                "  \n" +
                "  'connector.table-name' = 'test',  -- required: hbase table name\n" +
                "  \n" +
                "  'connector.zookeeper.quorum' = '172.20.184.17:2181', -- required: HBase Zookeeper quorum configuration\n" +
                "\n" +
                "  'connector.write.buffer-flush.max-size' = '10mb', -- optional: writing option, determines how many size in memory of buffered\n" +
                "                                                    -- rows to insert per round trip. This can help performance on writing to JDBC\n" +
                "                                                    -- database. The default value is \"2mb\".\n" +
                "\n" +
                "  'connector.write.buffer-flush.max-rows' = '1000', -- optional: writing option, determines how many rows to insert per round trip.\n" +
                "                                                    -- This can help performance on writing to JDBC database. No default value,\n" +
                "                                                    -- i.e. the default flushing is not depends on the number of buffered rows.\n" +
                "\n" +
                "  'connector.write.buffer-flush.interval' = '2s'   -- optional: writing option, sets a flush interval flushing buffered requesting\n" +
                "                                                    -- if the interval passes, in milliseconds. Default value is \"0s\", which means\n" +
                "                                                    -- no asynchronous flush thread will be scheduled.\n" +
                ")";

        tableEnvironment.sqlUpdate(createTable);

        String query = "SELECT user,SUM(age) as countage FROM MyUserTable group by user";
        Table table = tableEnvironment.sqlQuery(query);

        DataStream<Tuple2<Boolean, UserVo>> rowDataStream = tableEnvironment.toRetractStream(table, UserVo.class);
        rowDataStream.print();
        environment.execute();
    }
}
