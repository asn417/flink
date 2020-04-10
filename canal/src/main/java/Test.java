import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * @Author: wangsen
 * @Date: 2020/4/7 16:26
 * @Description:
 **/
public class Test {
    private static Logger logger = LoggerFactory.getLogger(Test.class);

    private static Config kafka_config = ConfigService.getConfig("bigData.kafka");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().registerTypeWithKryoSerializer(Message.class, DefaultSerializers.StringSerializer.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config.getProperty("bootstrap.servers","172.20.184.17:9092"));
        DataStream<Message> stream = env
                .addSource(new FlinkKafkaConsumer<>(kafka_config.getProperty("kafka.topic","test_topic"),
                        new MyCustomDeserializationSchema(), properties));
        //这里可以考虑将接收到的message解析成pojo，然后使用table SQL处理
        stream.map(new MapFunction<Message, String>() {
            @Override
            public String map(Message message) throws Exception {
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId != -1 && size != 0) {
                    List<CanalEntry.Entry> entries = message.getEntries();
                    for(CanalEntry.Entry entry : entries){
                        logger.info("==================== the binlog table is {}=====================",entry.getHeader().getTableName());
//                        if(StringUtils.isNotEmpty(entry.getHeader().getTableName()) && entry.getHeader().getTableName().equalsIgnoreCase(
//                                kafka_config.getProperty("tableNames","t_cc_transferbill"))){
//                        }
                    }
                }
                return "--------message.getEntries().size() == 0--------";
            }
        });
        env.execute("binlog-kafka-flink");
    }
}
