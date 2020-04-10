package kafka;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * @Author: wangsen
 * @Date: 2020/4/9 11:03
 * @Description:
 **/
public class CanalKafka {
    private static Logger logger = LoggerFactory.getLogger(CanalKafka.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //注册序列化器，否则报错com.esotericsoftware.kryo.KryoException: java.lang.UnsupportedOperationException
        env.getConfig().registerTypeWithKryoSerializer(Message.class, DefaultSerializers.StringSerializer.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "flink1:9092");

        DataStreamSource<Message> streamSource = env.addSource(new FlinkKafkaConsumer<>("example", new MyCustomDeserializationSchema(), properties));

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<Message, String>() {
            @Override
            public String map(Message message) throws Exception {
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId != -1 && size != 0) {
                    List<CanalEntry.Entry> entries = message.getEntries();
                    for (CanalEntry.Entry entry : entries) {
                        logger.info("==================== the binlog table is {}=====================", entry.getHeader().getTableName());
                    }
                }
                return "--------message.getEntries().size() == 0--------";
            }
        });

        map.print();
        env.execute("binlog-kafka-flink");
    }
}
