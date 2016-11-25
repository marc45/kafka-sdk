package com.sijifeng.kafka.javasdk;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.sijifeng.kafka.javasdk.thrift.Data;

import kafka.producer.ProducerConfig;
import kafka.producer.KeyedMessage;
public class KafkaProducer {
    private final kafka.javaapi.producer.Producer<String, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public KafkaProducer(String topic)
    {
      props.put("serializer.class", "kafka.serializer.StringEncoder");
      //props.put("zookeeper.connect", "192.168.78.48:2182");
      props.put("metadata.broker.list", "192.168.78.49:9092");
      ProducerConfig config = new ProducerConfig(props);
      // 创建producer
      producer = new kafka.javaapi.producer.Producer<String, String>(config);
      this.topic = topic;
    }
    
    public void run() {


      Data data = new Data();
      data.setEvent("test11");
      data.setHost("xyzs_httplog_server_tx_01");
      data.setTimestamp(1474979590);
      //data.setTimestamp(1474975590);

      Map<String, String> properties = new HashMap<>();
      properties.put("category","cqss");
      properties.put("cpu_average","86");
      properties.put("cpu1","100");
      properties.put("enumTest","muzhidao");

      data.setProperties(properties);

      for(int i=0;i<1;i++){
          producer.send(new KeyedMessage<String, String>(topic, data.toJson()));
      }

    }
    
    public static void main(String[] args) {
        KafkaProducer producer = new KafkaProducer("testtopic");
        producer.run();
    }
}
