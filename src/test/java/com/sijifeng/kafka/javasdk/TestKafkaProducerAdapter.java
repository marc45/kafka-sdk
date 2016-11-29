package com.sijifeng.kafka.javasdk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;

import com.sijifeng.kafka.javasdk.thrift.Data;

public class TestKafkaProducerAdapter {
    
    @Test
    public void NewkafkaProdecerTest(){
        
        KafkaProducerAdapter adapter = KafkaProducerAdapter.getInstance();
        KafkaConfig kafkaConfig = new KafkaConfig("192.168.78.48:9092");
        try {
            long p1 = System.currentTimeMillis();
            adapter.init(kafkaConfig);
            System.out.println("init 耗时"+(System.currentTimeMillis() - p1));
            Map<String, String> dataProps = new HashMap<String, String>();
            dataProps.put("category", "category_test");
            dataProps.put("test11", "56");
            Data data = new Data("host11", "199", 1452541545, new ArrayList<>(), dataProps);
            
            long t1 = System.currentTimeMillis();
            adapter.send("testtopic", data);
            long t2 = System.currentTimeMillis();
            System.out.println("耗时1"+(t2 - t1));
            adapter.send("testtopic", data);
            long t3 = System.currentTimeMillis();
            System.out.println("耗时2"+(t3 - t2));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void Test() {
        Properties kafkaProperties = new Properties();
        List<String> kafkaServers = new ArrayList<String>();
        String topic = "test";
        String msg = "12345";
        String key = "123";
        kafkaServers.add("192.168.78.48:9092");
        kafkaProperties.put("bootstrap.servers", kafkaServers);
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("acks", "1");
        kafkaProperties.put("retries", 1);
        kafkaProperties.put("max.request.size", 100000);
        kafkaProperties.put("client.id", "scribe_producer");
        long t1 = System.currentTimeMillis();
        System.out.println("开始初始化："+ t1);
        
        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(kafkaProperties);
        long t2 = System.currentTimeMillis();
        System.out.println("结束初始化："+ t2);
        System.out.println("耗时 = "+(t2 - t1));
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, 0, key, msg);
        kp.send(record);
        kp.close();
    }
    

}
