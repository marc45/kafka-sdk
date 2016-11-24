package com.sijifeng.kafka.javasdk;


import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import kafka.producer.KeyedMessage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sijifeng.kafka.javasdk.thrift.Data;


public class KafkaSink extends AbstractSink{
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    
    private List<KeyedMessage<String, String>> messageList;
    private static Properties props = new Properties();

    // 是否已初始化
    private static volatile boolean isInit = false;

    // 发送消息的默认编码，如果没有设置编码，则使用该默认编码
    private static final String defaultCharsetName = "utf-8";

    // 发送消息的编码
    private static Charset charset;

    // 如果消息发送失败，尝试发送的消息次数，默认为3次
    private static int attemptTimes = 3;
    
    static KafkaProducer producer;

    /**
     * 初始化客户端配置 客户端配置必须放在classpath根目录下的kafka-client.properties文件中
     */
    public synchronized static void init() {
        if(isInit){
            return;
        }
        logger.info("初始化配置kafka-client.properties");
        
        // 读取配置文件
        InputStream is = KafkaSink.class.getClassLoader().getResourceAsStream("kafka-client.properties");

        if (is == null) {
            logger.error("找不到配置文件kafka-client.properties");
            return;
        }

        try {
            props.load(is);
            // 从配置文件中读取消息编码
            String charsetName = props.getProperty("charset");
            if (charsetName != null && !charsetName.equals("")) {
                try {
                    charset = Charset.forName(charsetName);
                } catch (Exception e) {
                    logger.error("编码charset=" + charsetName + "初始化失败，使用默认编码charset=" + defaultCharsetName, e);
                }
            }
            props.remove("charset");

            // 如果编码为空，则使用默认编码utf-8
            if (charset == null) {
                try {
                    charset = Charset.forName(defaultCharsetName);
                } catch (Exception e) {
                    logger.error("默认编码charset=" + defaultCharsetName + "初始化失败", e);
                }
            }

            // 读取消息发送次数配置
            String strAttemptTimes = props.getProperty("attemptTimes");
            if (strAttemptTimes != null && !strAttemptTimes.equals("")) {
                int tmpAttemptTimes = 0;
                try {
                    tmpAttemptTimes = Integer.parseInt(strAttemptTimes);
                } catch (NumberFormatException e) {
                    logger.error("消息发送次数attemptTimes=" + strAttemptTimes + "初始化失败，使用默认发送次数attemptTimes=" + attemptTimes, e);
                }

                if (tmpAttemptTimes > 0) {
                    attemptTimes = tmpAttemptTimes;
                }
            }
            props.remove("attemptTimes");

            // 初始化Kafka Client
            //client = RpcClientFactory.getInstance(props);

            isInit = true;
        } catch (IOException e) {
            logger.error("配置文件kafka-client.properties读取失败", e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        }
    }
    
    
    public void sendData(List<Data> data){
        for (final Data d : data) {
            //producer.send(new KeyedMessage<String, String>("topic", d.toJson()));
        }
    }
    
    private void process(){
        try {
            
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
    
    
    
    protected void open() {
        Properties properties = new Properties();
        /*properties.put("zk.connect", Global.kafkaZookeeper);
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", Global.kafkaBroker);
        ProducerConfig config = new ProducerConfig((properties));
        producer = new Producer<String, String>(config);*/

    }

    protected void close() {

    }
    
    
}
