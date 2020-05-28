package stream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(3);
        //environment.enableCheckpointing(5000);//5秒进行一次checkpoint，便于失败容错
        //environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//设置检查语义位exactly_once
        //表示任务cancel后保留CheckPoint数据，以便恢复
        //environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092");
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG,"con1");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("test-topic",new SimpleStringSchema(),properties);

        //每次启动都会从所有partition中最早的offset开始消费。
        //但如果是从checkpoint或savepoint恢复的话，则会根据恢复的状态中保存的offset开始读。
        //consumer.setStartFromEarliest();

        //每次启动后，不会消费以前的消息，只会消费启动后生产的消息。
        //但如果是从checkpoint或savepoint恢复的话，则会根据恢复的状态中保存的offset开始读。
        //consumer.setStartFromLatest();

        //指定从消费者组中获取到的offset开始消费，因此必须在Properties中配置group.id。如果没有设置group.id，则相当于是setStartFromLatest方法，重启后会从最新的消息开始消费。
        //但如果是从checkpoint或savepoint恢复的话，则会根据恢复的状态中保存的offset开始读。
        //consumer.setStartFromGroupOffsets();//比如在消费完aaa之后服务停掉了，后面kafka又生产了bbb，此时重启服务，则会从bbb开始消费

        //根据指定的topic、partition以及offset的下一位开始消费
        KafkaTopicPartition kafkaTopicPartition = new KafkaTopicPartition("test-topic",0);
        Map<KafkaTopicPartition,Long> map = new HashMap<>();
        map.put(kafkaTopicPartition,1l);
        consumer.setStartFromSpecificOffsets(map);

        DataStreamSource<String> source = environment.addSource(consumer);

        source.print().setParallelism(3);

        environment.execute("consumer011-->");


    }
}
