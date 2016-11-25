package com.sijifeng.kafka.javasdk;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.serializer.StringEncoder;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sijifeng.kafka.javasdk.thrift.Data;


public class KafkaNewProducerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaNewProducerAdapter.class);
    
    private static KafkaProducer kafkaProducer = null;

    private static KafkaNewProducerAdapter kafkaProducerAdapter = null;

    private KafkaNewProducerAdapter() { }
    
    /**
     * 单例
     * @return
     */
    public static KafkaNewProducerAdapter getInstance() {
        if (kafkaProducerAdapter == null) {
            synchronized (KafkaNewProducerAdapter.class) {
                if (kafkaProducerAdapter == null) {
                    kafkaProducerAdapter = new KafkaNewProducerAdapter();
                }
            }
        }
        return kafkaProducerAdapter;
    }

    public void init (KafkaConfig kafkaConfig) throws Exception {
        try {
            Properties props = new Properties();
            
            List<String> kafkaServers = new ArrayList<String>();
            for(String kafkaServer : kafkaConfig.brokerLists.split(",")) {
                if(kafkaServer != null && kafkaServer.contains(":")) {
                    kafkaServers.add(kafkaServer);
                }
            }
            props.put("bootstrap.servers", kafkaServers);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("acks", "1");
            props.put("retries", 1);
            props.put("max.request.size", 100000);
            props.put("client.id", "DemoProducer");
          
            kafkaProducer = new KafkaProducer<>(props);
        } catch (Exception e) {
            throw new Exception("init kafka producer exception:" + e.getMessage());
        }
    }
    
    public void send(String topic, List<Data> datas) {
        ProducerRecord record;
        for(Data data : datas) {
            if(null != data){
                record = new ProducerRecord<>(topic, "", data.toJson());
                kafkaProducer.send(record, new SendCallback(record, 0));
            }
        }
    }
    
    public void send(String topic, Data data) {
        ProducerRecord record;
        if(null != data){
            record = new ProducerRecord<>(topic, null, data.toJson());
            kafkaProducer.send(record);
        }
    }
    
    /**
     * 发送一条消息
     * @param message
     */
    public void send(String topic, String message) {
        ProducerRecord record;
        record = new ProducerRecord<>(topic, "", message);
        kafkaProducer.send(record, new SendCallback(record, 0));
    }

    /**
     * producer回调
     */
    static class SendCallback implements Callback {
        ProducerRecord<String, String> record;
        int sendSeq = 0;

        public SendCallback(ProducerRecord record, int sendSeq) {
            this.record = record;
            this.sendSeq = sendSeq;
        }
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            e.printStackTrace();
            //send success
            /*if (null == e) {
                String meta = "topic:" + recordMetadata.topic() + ", partition:"
                        + recordMetadata.topic() + ", offset:" + recordMetadata.offset();
                logger.info("send message success, record:" + record.toString() + ", meta:" + meta);
                return;
            }
            //send failed
            logger.error("send message failed, seq:" + sendSeq + ", record:" + record.toString() + ", errmsg:" + e.getMessage());
            if (sendSeq < 1) {
                kafkaProducer.send(record, new SendCallback(record, ++sendSeq));
            }*/
        }
    }
    
}
