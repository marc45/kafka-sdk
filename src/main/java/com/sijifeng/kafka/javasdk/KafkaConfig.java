package com.sijifeng.kafka.javasdk;

import java.io.Serializable;

public class KafkaConfig implements Serializable{
    private static final long serialVersionUID = -5121200763583181826L;
    
    public final String brokerLists;
    public final String zkConnect;
    
    //是否获取反馈
    //0是不获取反馈(消息有可能传输失败)
    //1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
    //-1是所有in-sync replicas接受到消息时的反馈
    public int request_required_acks = 1;
    public int attemptTimes = 3;
    
    public KafkaConfig(String brokerLists, String zkConnect){
        this.brokerLists = brokerLists;
        this.zkConnect = zkConnect;
    }
    
    public KafkaConfig(String brokerLists, String zkConnect, int attemptTimes){
        this(brokerLists, zkConnect);
        this.attemptTimes = attemptTimes;
    }
    
    public KafkaConfig(String brokerLists, String zkConnect, int attemptTimes, int request_required_acks){
        this(brokerLists, zkConnect);
        this.attemptTimes = attemptTimes;
        this.request_required_acks = request_required_acks;
    }

}
