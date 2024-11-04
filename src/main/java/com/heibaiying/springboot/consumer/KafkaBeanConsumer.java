package com.heibaiying.springboot.consumer;

import com.alibaba.fastjson.JSON;
import com.heibaiying.springboot.bean.Programmer;
import com.heibaiying.springboot.constant.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author : heibaiying
 * @description : kafka 消费者
 */

@Component
@Slf4j
public class KafkaBeanConsumer {
    // 发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.bean, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value={"birthday":1560852963470,"name":"xiaoming","salary":21212.33,"age":12}, timestamp=null), recordMetadata=spring.boot.kafka.bean-0@1]
    // 消费者收到消息:Programmer(name=xiaoming, age=12, salary=21212.33, birthday=null)
	/*
	 * 发送结果:SendResult [producerRecord=ProducerRecord(topic=spring.boot.kafka.bean, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value={"birthday":1560935191543,"name":"xiaoming","salary":21212.33,"age":12}, timestamp=null), recordMetadata=spring.boot.kafka.bean-0@4]
                 消费者收到消息:com.heibaiying.springboot.bean.Programmer@321cf202 {"birthday":1560935191543,"name":"xiaoming","salary":21212.33,"age":12}
	 * */
    @KafkaListener(groupId = "beanGroup", topics = Topic.BEAN, concurrency = "1")
    public void consumer(ConsumerRecord<String, Object> record) {
        System.out.println("消费者收到消息:" + JSON.parseObject(record.value().toString(), Programmer.class) + " " + record.value().toString());
    }
}
