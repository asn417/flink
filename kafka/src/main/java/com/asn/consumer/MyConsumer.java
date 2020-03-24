package com.asn.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        //1.配置
        Properties properties = new Properties();
        //kafka集群配置
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"flink1:9092,flink2:9092,flink3:9092");
        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        //设置自动提交的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //key value的反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"bigdata");

        //2.创建消费者
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(properties);

        //3.订阅主题
        consumer.subscribe(Arrays.asList("second"));


        while (true) {
            //4.拉取数据
            ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofMinutes(1));
            //5.解析数据
            for (ConsumerRecord consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "---" + consumerRecord.value());
            }
        }

        //6关闭连接资源
        //consumer.close();
    }
}
