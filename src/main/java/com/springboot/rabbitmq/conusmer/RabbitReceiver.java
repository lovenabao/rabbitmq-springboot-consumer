package com.springboot.rabbitmq.conusmer;

import com.rabbitmq.client.Channel;
import com.springboot.rabbitmq.entity.Order;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author shkstart
 * @create 2019-06-21 14:36
 */
@Component
public class RabbitReceiver {

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "queue_1",durable = "true"),
            exchange = @Exchange(value = "exchange_1",type = "topic",ignoreDeclarationExceptions = "true",durable = "true"),
            key = "springboot.*"
    ))
    @RabbitHandler
    public void onMessage(Message message, Channel channel)throws Exception{
        System.out.println("Conusmer Message :  " + message.getPayload());
        Long delivertTag = (Long)message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //手工ACK
        channel.basicAck(delivertTag,false);
    }

    /**
     *
     *
     spring.rabbitmq.listener.order.queue.name=queue-2
     spring.rabbitmq.listener.order.queue.durable=true
     spring.rabbitmq.listener.order.exchange.name=exchange-1
     spring.rabbitmq.listener.order.exchange.durable=true
     spring.rabbitmq.listener.order.exchange.type=topic
     spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions=true
     spring.rabbitmq.listener.order.key=springboot.*
     * @param order
     * @param channel
     * @param headers
     * @throws Exception
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "${spring.rabbitmq.listener.order.queue.name}",
                    durable = "${spring.rabbitmq.listener.order.queue.durable}"),
            exchange = @Exchange(value = "${spring.rabbitmq.listener.order.exchange.name}",
                    type = "${spring.rabbitmq.listener.order.exchange.type}",
                    ignoreDeclarationExceptions = "${spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions}",
                    durable = "${spring.rabbitmq.listener.order.exchange.durable}"),
            key = "${spring.rabbitmq.listener.order.key}"
    ))
    @RabbitHandler
    public void onOrderMessage(@Payload Order order, Channel channel,
                               @Headers Map<String,Object> headers)throws Exception{
        System.out.println("=================================");
        System.out.println("消费端order : " + order.getId());
        Long delivertTag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
        channel.basicAck(delivertTag,false);
    }

}
