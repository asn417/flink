package com.asn.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class MyProducerWithPartitioner {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"flink1:9092,flink2:9092,flink3:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //添加自定义分区类
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.asn.producer.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("second","hello-->"+i),new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(recordMetadata.partition()+"---"+recordMetadata.offset());
                }
            });
        }
        producer.close();
    }
}
