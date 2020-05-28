package stream;

import com.alibaba.otter.canal.protocol.Message;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stream.mysink.HbaseProcess;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author: wangsen
 * @Date: 2020/5/26 16:11
 * @Description: 测试读取kafka，且通过checkpoint机制实现exactly once语义.
 * flink kafka consumer需要借助checkpoint来实现exactly once语义。
 * 通过开启checkpoint来实现间隔的保存算子状态和offset。
 **/
public class TestKafkaCheckpoint {
    private static Logger logger = LoggerFactory.getLogger(TestKafkaCheckpoint.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置checkpoint的周期，每隔1秒进行一次checkpoint。并设置exactly once语义。
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //设置StateBackend存储路径。后面的true表示每次增量同步checkpoint，而不是全量同步一遍。提高性能
        env.setStateBackend(new RocksDBStateBackend("hdfs://master:9000/flink/checkpoints",true));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //下面注释掉后，cancel job后会清理掉meta data and actual program state
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);


        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.184.17:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"test-groupid");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test-topic", new SimpleStringSchema(), properties);
        consumer.setStartFromGroupOffsets();//默认的
        consumer.setCommitOffsetsOnCheckpoints(true);//默认就是true

        DataStream<String> stream = env.addSource(consumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("===== print value:"+value+" =====");
                logger.info("===== logger value:"+value+" =====");
                return value;
            }
        });

        //stream.print("test-topic:");
        stream.process(new HbaseProcess());

        env.execute("test-topic-job");
    }
}
