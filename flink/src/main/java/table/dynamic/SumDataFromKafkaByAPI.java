package table.dynamic;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author: wangsen
 * @Date: 2020/4/10 15:46
 * @Description:
 **/
public class SumDataFromKafkaByAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.connect(new Kafka()
                .version("universal")    // required: valid connector versions are
                //   "0.8", "0.9", "0.10", "0.11", and "universal"
                .topic("test")       // required: topic name from which the table is read
                // optional: connector specific properties
                .property("zookeeper.connect", "master:2181")
                .property("bootstrap.servers", "master:9092")
                .property("group.id", "testGroup")
                // optional: select a startup mode for Kafka offsets
                //.startFromEarliest()
                .startFromLatest()
                //.startFromSpecificOffsets(...)
                // optional: output partitioning from Flink's partitions into Kafka's partitions
                //.sinkPartitionerFixed()         // each Flink partition ends up in at-most one Kafka partition (default)
                //.sinkPartitionerRoundRobin()    // a Flink partition is distributed to Kafka partitions round-robin
                //.sinkPartitionerCustom(MyCustom.class)    // use a custom FlinkKafkaPartitioner subclass
            )
            .withFormat(                                  // required: Kafka connector requires to specify a format,
                  new Json().failOnMissingField(true)                          // the supported formats are Csv, Json and Avro.
            ).withSchema(new Schema()
                //.field("user", "String")
                .field("user", DataTypes.STRING())
                //.field("age","BigDecimal")
                .field("age",DataTypes.DECIMAL(38,18))
        ).createTemporaryTable("MyUserTable");

        String query = "SELECT user,SUM(age) as countage FROM MyUserTable group by user";
        Table table = tableEnvironment.sqlQuery(query);
        DataStream<Tuple2<Boolean, UserVo>> rowDataStream = tableEnvironment.toRetractStream(table, UserVo.class);
        rowDataStream.print();
        environment.execute();
    }
}
