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

        String createTable =  "CREATE TABLE MyUserTable (\n" +
                "  `user` VARCHAR,\n" +
                "  age DECIMAL\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',       \n" +
                "\n" +
                "  'connector.version' = 'universal',     -- required: valid connector versions are\n" +
                "                                    -- \"0.8\", \"0.9\", \"0.10\", \"0.11\", and \"universal\"\n" +
                "\n" +
                "  'connector.topic' = 'test', -- required: topic name from which the table is read\n" +
                "\n" +
                "  'connector.properties.zookeeper.connect' = 'master:2181', -- required: specify the ZooKeeper connection string\n" +
                "  'connector.properties.bootstrap.servers' = 'master:9092', -- required: specify the Kafka server connection string\n" +
                "  'connector.properties.group.id' = 'testGroup', --optional: required in Kafka consumer, specify consumer group\n" +
                "  'connector.startup-mode' = 'latest-offset',    -- optional: valid modes are \"earliest-offset\", \n" +
                "                                                   -- \"latest-offset\", \"group-offsets\", \n" +
                "                                                   -- or \"specific-offsets\"\n" +
                "\n" +
                "  -- optional: used in case of startup mode with specific offsets\n" +
                "  -- 'connector.specific-offsets' = 'partition:0,offset:42;partition:1,offset:300',\n" +
                "\n" +
                "  -- 'connector.sink-partitioner' = '...',  -- optional: output partitioning from Flink's partitions \n" +
                "                                         -- into Kafka's partitions valid are \"fixed\" \n" +
                "                                         -- (each Flink partition ends up in at most one Kafka partition),\n" +
                "                                         -- \"round-robin\" (a Flink partition is distributed to \n" +
                "                                         -- Kafka partitions round-robin)\n" +
                "                                         -- \"custom\" (use a custom FlinkKafkaPartitioner subclass)\n" +
                "  -- optional: used in case of sink partitioner custom\n" +
                "  -- 'connector.sink-partitioner-class' = 'org.mycompany.MyPartitioner',\n" +
                "  \n" +
                "  'format.type' = 'json',                 -- required: Kafka connector requires to specify a format,\n" +
                "                                         -- the supported formats are 'csv', 'json' and 'avro'.\n" +
                "                                         -- Please refer to Table Formats section for more details.\n" +
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
