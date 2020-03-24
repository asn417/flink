package com.asn.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) throws InterruptedException {
        //1.创建生产者配置参数
        Properties properties = new Properties();
        //kafka集群，broker list
        properties.put("bootstrap.servers","flink1:9092,flink2:9092,flink3:9092");
        //properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"flink1:9092,flink2:9092,flink3:9092");
        //ACK应答级别(all就代表-1级别)
        properties.put("acks","all");
        //重试次数
        properties.put("retries",3);
        //批次大小，单位字节（当数据量达到批次后再发送到recordaccumulator）
        properties.put("batch.size",16384);
        //等待时间(就算是数据量没达到batch.size，到了时间也会发送)
        properties.put("linger.ms",1000);
        //RecordAccumulator缓冲区大小
        properties.put("buffer.memory",33554432);
        //key,value的序列化类
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //2.创建生产者对象（生产者的所有配置都是通过这个properties来初始化，包括拦截器之类的）
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //3.发送数据
        while (true){
            producer.send(new ProducerRecord<>("second","asn","hello-->"));
            Thread.currentThread().sleep(1000);
        }

        //4.关闭连接资源
        //producer.close();

    }
}
