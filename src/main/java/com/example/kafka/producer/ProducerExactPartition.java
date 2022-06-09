package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerExactPartition {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); //브로커 주소, 포트
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //KEY 시리얼라이즈 설정
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //VALUE 시리얼라이즈 설정

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        int partition = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partition, "Pangyo", "Pangyo"); //파티션을 지정
        producer.send(record); //전송할 데이터를 어큐뮬레이터에 저장한다.

        producer.flush(); //어큐뮬레이터에 저장되어 있는 데이터를 카프카 브로커로 전송한다.
        producer.close(); //프로듀서 리소스를 해제한다.
    }
}
