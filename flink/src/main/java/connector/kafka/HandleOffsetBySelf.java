package connector.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.shaded.curator.org.apache.curator.RetryPolicy;
import org.apache.flink.shaded.curator.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper.org.apache.zookeeper.CreateMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.*;

/**
 * @Author: wangsen
 * @Date: 2020/5/28 12:50
 * @Description: 手动提交的方式维护消费的offset，消费一条就更新一下offset到zookeeper。如果宕机，重启后还是可以从zookeeper获取上次消费的offset，接着往下消费。
 * 这样能解决checkpoint端到端的exactly once问题（但只是实时更新offset，并不会维护算子的状态，因此如果涉及到中间迭代计算的话，还是得考虑使用checkpoint，然后sink端自定义两阶段提交）。
 * 这样可以关闭自动周期性提交enable.auto.commit=false。
 **/
public class HandleOffsetBySelf {
    //会话超时时间
    private static final int SESSION_TIMEOUT = 30 * 1000;

    //连接超时时间
    private static final int CONNECTION_TIMEOUT = 3 * 1000;

    //ZooKeeper服务地址
    private static final String CONNECT_ADDR = "172.20.184.17";

    //创建连接实例
    private static CuratorFramework client;

    public static void main(String[] args) throws Exception {
        //1 重试策略：初试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //2 通过工厂创建连接
        client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR).connectionTimeoutMs(CONNECTION_TIMEOUT)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
//                .namespace("super") //命名空间
                .build();
        //3 开启连接
        client.start();
        //获取流环境
        StreamExecutionEnvironment flinkEnv = changeEnv();

        //从zk获取offset
        //Tuple2<HashMap<KafkaTopicPartition, Long>, Boolean> kafkaOffset = getOffsets("test-topic", "test");

        FlinkKafkaConsumer<KafkaSource> ds = createKafkaSource("test-topic", "test");
        //如果从zk中获取到了offset，或者是从checkpoint恢复，则这个设置会被忽略
        //setStartFromGroupOffsets()这个方法会获取指定的消费者组的offset，如果对应的消费者组还没有提交offset，则会根据auto.offset.reset获取
        FlinkKafkaConsumerBase flinkKafkaConsumerBase = ds.setStartFromGroupOffsets();

        // 如果kafka不为空的话，从这里开始执行
        /*if (kafkaOffset.f1) {
            System.out.println("----------------------zookeeper manager offsets-----------------------------------");
            Map<KafkaTopicPartition, Long> specificStartOffsets = kafkaOffset.f0;
            //这个方法会忽略掉自动提交的offset。因此这个方法可以与自动提交或checkpoint一起使用。
            //如果从zk获取到offset，则使用此方法。如果没有，则使用自动提交或checkpoint的方式。
            flinkKafkaConsumerBase = ds.setStartFromSpecificOffsets(specificStartOffsets);
        }*/
        DataStreamSource<KafkaSource> tetsds = flinkEnv.addSource(flinkKafkaConsumerBase);
        tetsds.print();
//        tetsds.print();
        flinkEnv.execute("test");
    }


    public static void ensureZKExists(String zkTopicPath) {
        try {
            if (client.checkExists().forPath(zkTopicPath) == null) {//zk中没有没写过数据，创建父节点，也就是会递归创建
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)  // 节点类型
                        .forPath(zkTopicPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void storeOffsets(HashMap<String, Long> offsetRange, String topic, String group) {
        String zkTopicPath = String.format("/offsets/%s/%s", topic, group);
        Iterator<Map.Entry<String, Long>> setoffsetrange = offsetRange.entrySet().iterator();

        while (setoffsetrange.hasNext()) {
            Map.Entry<String, Long> offsethas = setoffsetrange.next();
            //partition
            String path = String.format("%s/%s", zkTopicPath, offsethas.getKey());
            ensureZKExists(path);
            try {
                client.setData().forPath(path, (offsethas.getValue() + "").getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从zookeeper中读取kafka对应的offset
     *
     * @param topic
     * @param group
     * @return Tuple2<HashMap < TopicPartition, Long>, Boolean>
     */
    public static Tuple2<HashMap<KafkaTopicPartition, Long>, Boolean> getOffsets(String topic, String group) {
        ///xxxxx/offsets/topic/group/partition/
        String zkTopicPath = String.format("/offsets/%s/%s", topic, group);

        //判断zk上是否存在节点，不存在则自动创建
        ensureZKExists(zkTopicPath);
        HashMap<KafkaTopicPartition, Long> offsets = new HashMap<KafkaTopicPartition, Long>();
        try {
            List<String> partitions = client.getChildren().forPath(zkTopicPath);
            for (String partition : partitions) {
//                System.out.println(new String(client.getData().forPath(String.format("%s/%s", zkTopicPath,partition))));
                Long offset = Long.valueOf(new String(client.getData().forPath(String.format("%s/%s", zkTopicPath, partition))));
                KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, Integer.valueOf(partition));

                offsets.put(topicPartition, offset);
            }
            if (!offsets.isEmpty()) {
                return new Tuple2<>(offsets, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Tuple2<>(offsets, false);
    }


    public static Properties getKafkaProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.20.184.17:9092");
//        properties.setProperty("zookeeper.connect", getStrValue(new Constants().KAFKA_ZOOKEEPER_LIST));
        properties.setProperty("group.id", groupId);
        //auto.offset.reset：表示当broker中不存在相应的offset时，则选择一种方式重置从哪里开始读，有earliest、latest、none和anything。但最后两种会抛异常。
        //"none":Caused by: org.apache.kafka.clients.consumer.NoOffsetForPartitionException: Undefined offset with no reset policy for partitions: [test-topic-1]
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //如果为true，则会周期性的自动提交offset。（其实这个和checkpoint的功能是一样的，只不过checkpoint除了offset，还会保存算子的状态）
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");//如果手动提交，这里可以设为false
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        return properties;
    }

    public static Properties getProduceKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }


    public static StreamExecutionEnvironment changeEnv() throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*//env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().enableForceKryo();
        //启用检查点，设置检查点的最小间隔为60000ms
        env.setStateBackend(new RocksDBStateBackend("hdfs://master:9000/flink/checkpoints/test-topic",true));
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
        checkpointConfig.setCheckpointInterval(60000);
        // 设置模式为exactly-once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        checkpointConfig.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);*/
        return env;
    }

    /**
     * 创建kafka的source
     *
     * @param topic
     * @param groupid
     * @return
     */
    public static FlinkKafkaConsumer<KafkaSource> createKafkaSource(String topic, String groupid) {
        // kafka消费者配置
        FlinkKafkaConsumer<KafkaSource> dataStream = new FlinkKafkaConsumer<KafkaSource>(topic, new KeyedDeserializationSchema<KafkaSource>() {
            private static final long serialVersionUID = 234204640604818587L;

            @Override
            public TypeInformation<KafkaSource> getProducedType() {
                return TypeInformation.of(new TypeHint<KafkaSource>() {
                });
            }

            @Override
            public KafkaSource deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
                KafkaSource kafkasource = new KafkaSource();
                kafkasource.setTopic(topic);
                kafkasource.setMessage(Bytes.toString(message));
                kafkasource.setPartition(partition);
                kafkasource.setOffset(offset);
                HashMap<String, Long> partitionAndOffset = new HashMap<>();
                partitionAndOffset.put(String.valueOf(partition), offset);
                //实时保存offset到zk
                storeOffsets(partitionAndOffset, topic, groupid);
                System.out.println("==================="+Bytes.toString(message));
                return kafkasource;
            }

            @Override
            public boolean isEndOfStream(KafkaSource s) {
                return false;
            }
        }, getKafkaProperties(groupid));
        //设置消息的起始位置的偏移量,最晚的记录开始启动
        //dataStream.setStartFromLatest();
        //自动提交offset
//        dataStream.setCommitOffsetsOnCheckpoints(true);
        return dataStream;
    }
}
