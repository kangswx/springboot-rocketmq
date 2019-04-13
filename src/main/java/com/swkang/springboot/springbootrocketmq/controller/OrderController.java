package com.swkang.springboot.springbootrocketmq.controller;

import com.swkang.springboot.springbootrocketmq.domain.JsonData;
import com.swkang.springboot.springbootrocketmq.jms.MsgProducer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.UnsupportedEncodingException;

@RestController
@RequestMapping("/api/v1/jms")
public class OrderController {

    @Autowired
    private MsgProducer producer;

    /**
     * 微信支付回调接口
     * @param msg  支付信息
     * @param tag  消息二级分类
     * @return
     */
    @GetMapping("order")
    public Object order(String msg, String tag) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        System.out.println("tag: "+tag);
        System.out.println("msg: "+msg);
        //创建一个消息需要包含  topic, tag, 消息体
        Message message = new Message("testTopic", tag, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
        //发送消息
        SendResult sendResult = producer.getProducer().send(message);
        //打印发送结果
        System.out.println("发送响应：" + sendResult.getMsgId() + ",发送状态：" + sendResult.getSendStatus());
        return JsonData.buildSuccess();
    }
}
