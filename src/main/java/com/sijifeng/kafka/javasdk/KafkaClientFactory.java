package com.sijifeng.kafka.javasdk;


import kafka.producer.ProducerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaClientFactory {
    
    private static class LazyHolder { 
       /* Properties properties = new Properties();
        properties.put("zk.connect", "");
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", Global.kafkaBroker);
        ProducerConfig config = new ProducerConfig((properties));
        producer = new Producer<String, String>(config);*/
        //private static final KafkaProducer INSTANCE = new KafkaProducer();    
     }
    
    public KafkaClientFactory(){};
    
    //静态工厂方法   
    public static final KafkaProducer getInstance() {  
        //return LazyHolder.INSTANCE;
        return null;
    } 
}
