package com.swkang.springboot.springbootrocketmq.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MsgConsumer {

    @Value("${apache.rocketmq.consumer.PushConsumer}")
    private String consumerGroup;
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    private DefaultMQPushConsumer consumer;

    @PostConstruct
    public void defaultMQPushConsumer(){

        //消费者的组名
        consumer = new DefaultMQPushConsumer(consumerGroup);
        //多个地址以;隔开
        consumer.setNamesrvAddr(namesrvAddr);

        try {
            //设置consumer锁订阅的topic和tag,*表示全部的tag
//          consumer.subscribe("testTopic", "payment");
            consumer.subscribe("testTopic", "*");

            //CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，跳过历史消息
            //CONSUME_FROM_FIRST_OFFSET 从队列最开始消费，即历史消息(还存储在broker的)全部消费一遍
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

            //MessageListenerOrderly       这个是有序的
            //MessageListenerConcurrently  这个是无序的，并行的方式进行处理，效率高很多
            consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
                try {
                    for(MessageExt messageExt: list){
                        System.out.println("messageExt" + messageExt); //输出消息内容
                        String stringBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("消费响应：" + messageExt.getMsgId() + ", msgBody: " + stringBody); //输出消息内容
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;  //稍后再试
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  //消费成功
            });
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

    }
}
