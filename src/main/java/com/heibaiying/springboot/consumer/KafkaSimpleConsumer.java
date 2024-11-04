package com.heibaiying.springboot.consumer;

import com.heibaiying.springboot.constant.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author : heibaiying
 * @description : kafka 简单消息消费者
 */

@Component
@Slf4j
public class KafkaSimpleConsumer {

    // 简单消费者
    @KafkaListener(groupId = "simpleGroup", topics = Topic.SIMPLE)
    public void consumer1_1(ConsumerRecord<String, Object> record, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, Consumer consumer) {
        System.out.println("消费者收到消息:" + record.value() + "; topic:" + topic);
        Random random = new Random();
        int i = random.nextInt(2);
        int i1 = 6 / i;
        // offset
        System.out.println("partition:" + record.partition() + "; offset:" + record.offset());
//        consumer.commitSync();
        System.out.println("after:" + record.partition() + "; offset:" + record.offset());
        /*
	         * 发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.simple, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value=hello spring boot kafka, timestamp=null), recordMetadata=spring.boot.kafka.simple-0@4]
	        消费者收到消息:hello spring boot kafka; topic:spring.boot.kafka.simple
	         * 如果需要手工提交异步 consumer.commitSync();
	         * 手工同步提交 consumer.commitAsync()
         */
    }
}
