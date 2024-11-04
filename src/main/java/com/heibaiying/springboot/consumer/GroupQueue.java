package com.heibaiying.springboot.consumer;

import com.heibaiying.springboot.config.KafkaConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

/**
 * 消费者
 */
@Component
public class GroupQueue implements Runnable {
    protected KafkaConsumer consumer;

    public void run() {
        System.out.println("start");

        while (true) {
            try {
                if (consumer == null) {
                    consumer = KafkaConsumerFactory.getKafkaConsumer("test");
                    consumer.subscribe(Arrays.asList("example"));
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.count() <= 0) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value().equals("000")) {
                        System.out.println("ddddddcccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
                        throw new IllegalArgumentException("dddd");
                    }
                    System.out.printf(record.topic() +
                                    "一条新消息 offset = %d, key = %s, value = %s", record.offset(),
                            record.key(), record.value());
                    System.out.println(record.topic() + "partition:" +
                            record.partition());

                    // 业务处理 TODO
                }

                // 同步提交
                consumer.commitSync();
                System.out.println("批次提j 交");
            } catch (Exception e) {
                System.out.println("start consumer exception");
                break;
            }
        }
    }

}
