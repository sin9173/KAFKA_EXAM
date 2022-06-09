package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerSyncCallback {
    private final static Logger logger = LoggerFactory.getLogger(ProducerSyncCallback.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.ACKS_CONFIG, "0");
        //ACKS옵션을 0으로 할 경우 오프셋번호를 알 수 없음 (오프셋을 -1로 반환)
        //ACKS옵션을 1로 할 경우 리더파티션에 대해서만 결과를 가져온다.
        //acks가 -1(all)일경우 신뢰도가 높지만 속도는 느리다.

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", "Pangyo");

        try {
            RecordMetadata metadata = producer.send(record).get(); //전송 결과를 가져옴 토픽-파티션@오프셋
            logger.info(metadata.toString());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }


        producer.flush();
        producer.close();
    }
}
