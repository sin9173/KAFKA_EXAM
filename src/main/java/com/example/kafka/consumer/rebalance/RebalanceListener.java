package com.example.kafka.consumer.rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) { //리밸런스가 끝난 뒤에 파티션이 할당 완료되면 호출
        logger.warn("Partitions are assigned : " + partitions.toString());
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) { 
        //리밸런스가 시작되기 직전에 호출되는 메소드, 
        // 마지막으로 처리한 레코드를 기준으로 커밋을 하기 위해서는 리밸런스가 시작하기 직전에 커밋을 하면 되므로 onPartitionRevoked() 메소드에 커밋을 구현하여 처리
        logger.warn("Partitions are revoked : " + partitions.toString());
    }
}
