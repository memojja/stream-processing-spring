package com.springkafka.demo;

import org.apache.catalina.LifecycleState;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Configuration
public class Consumer {

    @Value("${brokerList}")
    private String brokerList;

    @Value("${topic}")
    private String topic;

    private List<Vote> votes;

    private KafkaConsumer<String, String> kafkaConsumer;

    @PostConstruct
    public void init(){

        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", brokerList);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "false");
//        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");

        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        if(votes==null)
            votes = new ArrayList<>();

    }

    public List<Vote> getAllVotes(){

        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

//                consumerRecord.value();
//                consumerRecord.key();
//                consumerRecord.offset();
//                consumerRecord.partition();
//                consumerRecord.topic();
//                consumerRecord.timestamp();

                System.out.println("Partition: " + consumerRecord.partition() +
                        ", Offset: " + consumerRecord.offset() +
                        ", Key: " + consumerRecord.key() +
                        ", Value: " + consumerRecord.value());
                votes.add(new Vote(consumerRecord.value()));
                kafkaConsumer.commitSync();

                return votes;

            }
        }



    }
}
