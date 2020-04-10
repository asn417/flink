package table.dynamic;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Author: wangsen
 * @Date: 2020/4/10 10:21
 * @Description:
 **/
public class SumDataFromKafkaByDDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        String createTable = "CREATE TABLE MyUserTable (\n" +
                "  `user` VARCHAR,\n" +
                "  age DECIMAL\n" +
                ") WITH (\n" +
                "  -- declare the external system to connect to\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'test',\n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "  'connector.properties.0.value' = 'master:2181',\n" +
                "  'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "  'connector.properties.1.value' = 'master:9092',\n" +
                "  'update-mode' = 'append',\n" +
                "  -- declare a format for this system\n" +
                "  'format.type' = 'json',\n" +
                "  'format.fail-on-missing-field' = 'true',\n"+
                "  'format.json-schema' = '{\n" +
                "                            \"type\": \"object\",\n" +
                "                            \"properties\": {\n" +
                "                                \"user\": {\"type\": \"string\"},\n" +
                "                                \"age\": {\"type\": \"number\"}\n" +
                "                            }\n" +
                "                         }'\n" +
                ")";

        tableEnvironment.sqlUpdate(createTable);

        String query = "SELECT user,SUM(age) as countage FROM MyUserTable group by user";
        Table table = tableEnvironment.sqlQuery(query);

        DataStream<Tuple2<Boolean, UserVo>> rowDataStream = tableEnvironment.toRetractStream(table, UserVo.class);
        rowDataStream.print();
        environment.execute();
    }
}
