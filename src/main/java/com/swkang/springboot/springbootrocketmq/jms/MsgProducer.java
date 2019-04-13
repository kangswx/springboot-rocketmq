package com.swkang.springboot.springbootrocketmq.jms;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MsgProducer {

    //生产者组名
    @Value("${apache.rocketmq.producer.producerGroup}")
    private String producerGroup;
    //namesrv地址
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    private DefaultMQProducer producer;

    public DefaultMQProducer getProducer() {
        return this.producer;
    }

    @PostConstruct
    public void defaultMQProducer(){
        producer = new DefaultMQProducer(producerGroup);
        //多个地址以;隔开
        producer.setNamesrvAddr(namesrvAddr);
        producer.setVipChannelEnabled(false);

        try {
            //producer在使用之前必须要调用start初始化，只能初始化一次
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        // producer.shutdown();
    }
}
