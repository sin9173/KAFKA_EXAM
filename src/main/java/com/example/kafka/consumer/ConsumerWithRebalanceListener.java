package com.example.kafka.consumer;

import com.example.kafka.consumer.rebalance.RebalanceListener;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithRebalanceListener {
    private static Logger logger = LoggerFactory.getLogger(ConsumerRebalanceListener.class);
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String TOPIC_NAME = "test";
    private static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener()); //subscribe 호출시에 리밸런스 리스너 추가

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
        }
    }
}
