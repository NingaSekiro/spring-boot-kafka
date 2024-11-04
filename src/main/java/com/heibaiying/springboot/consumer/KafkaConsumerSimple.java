package com.heibaiying.springboot.consumer;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.spi.LoggerContext;
import org.slf4j.LoggerFactory;

public class KafkaConsumerSimple {


    // 设置服务器地址
    private static final String bootstrapServer = "127.0.0.1:9092";

    // 设置主题
    private static final String topic = "spring.boot.kafka.simple";

    // 设置主题
    private static final String topic2 = "topic-demo2";

    // 设置消费者组
    private static final String groupId = "simpleGroup";

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 设置反序列化key参数信息
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 设置反序列化value参数信息
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置服务器列表信息，必填参数，该参数和生产者相同，，制定链接kafka集群所需的broker地址清单，可以设置一个或者多个
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        // 设置消费者组信息，消费者隶属的消费组，默认为空，如果设置为空，则会抛出异常，这个参数要设置成具有一定业务含义的名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // 制定kafka消费者对应的客户端id，默认为空，如果不设置kafka消费者会自动生成一个非空字符串。
        properties.put("client.id", "consumer.client.id.demo");

        // 设置每次从最早的offset开始消费
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 手动提交开启
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 将参数设置到消费者参数中
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 消息订阅
        // consumer.subscribe(Collections.singletonList(topic));
        // 可以订阅多个主题
        // consumer.subscribe(Arrays.asList(topic, topic2));
        // 可以使用正则表达式进行订阅
        // consumer.subscribe(Pattern.compile("topic-demo*"));

        // 指定订阅的分区
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        // 初始化offset位移为-1
        long lastConsumeOffset = -1;
        while (true) {
            try {
                // 每隔一秒监听一次，拉去指定主题分区的消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    // 获取到消息
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                    Random random = new Random();
                    int i = random.nextInt(2);
                    int i1 = 6 / i;
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("消费者收到消息: " + record.value() + "; topic: " + record.topic() + "; partition: " + record.partition() + "; offset: " + record.offset());
                    }
                    // 获取到消息的offset位移信息，最后消费的位移
                    lastConsumeOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    // 同步提交消费位移
//                    consumer.commitSync();
                    System.out.println("提交后的offset: " + lastConsumeOffset);
                }
            } catch (Exception e) {
                System.err.println("发生异常: " + e.getMessage());
                e.printStackTrace();
            }
        }
//        // 当前消费者最后一个消费的位置
//        System.out.println("consumed offset is " + lastConsumeOffset);
//        // 提交，下次消费从哪个位置开始
//        OffsetAndMetadata committed = consumer.committed(topicPartition);
//        System.out.println("committed offset is " + committed.offset());
//        // 下次消费从哪个位置开始
//        long position = consumer.position(topicPartition);
//        System.out.println("the offset of the next record is " + position);

    }

}