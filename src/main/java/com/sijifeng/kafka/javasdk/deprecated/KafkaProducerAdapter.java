package com.sijifeng.kafka.javasdk.deprecated;


import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sijifeng.kafka.javasdk.thrift.Data;


public class KafkaProducerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerAdapter.class);
    
    private static Producer<String, String> producer = null;

    private static KafkaProducerAdapter kafkaProducerAdapter = null;

    private KafkaProducerAdapter() { }
    
    /**
     * 单例
     * @return
     */
    public static KafkaProducerAdapter getInstance() {
        if (kafkaProducerAdapter == null) {
            synchronized (KafkaProducerAdapter.class) {
                if (kafkaProducerAdapter == null) {
                    kafkaProducerAdapter = new KafkaProducerAdapter();
                }
            }
        }
        return kafkaProducerAdapter;
    }

    public void init (KafkaConfig kafkaConfig) throws Exception {
        try {
            Properties props = new Properties();
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("metadata.broker.list", "192.168.78.48:9092");
            producer = new Producer<String, String>(new ProducerConfig(props));
        } catch (Exception e) {
            throw new Exception("init kafka producer exception:" + e.getMessage());
        }
    }
    
    public void send(String topic, List<Data> datas) {
        List<KeyedMessage<String, String>> messages = new LinkedList<KeyedMessage<String, String>>();
        for(Data data : datas) {
            if(null != data){
                messages.add(new KeyedMessage<String, String>(topic, data.toJson()));
            }
        }
        producer.send(messages);
    }
    
    public void send(String topic, Data data) {
        if(null != data){
            producer.send(new KeyedMessage<String, String>(topic, data.toJson()));
        }
    }
    
    /**
     * 发送一条消息
     * @param message
     */
    public void send(String topic, String message) {
        producer.send(new KeyedMessage<String, String>(topic, message));
    }
    


}
