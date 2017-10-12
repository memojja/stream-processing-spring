package com.springkafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Configuration
public class Producer {

    @Value("${brokerList}")
    private String brokerList;
    @Value("${sync}")
    private String sync;
    @Value("${topic}")
    private String topic;

    private org.apache.kafka.clients.producer.Producer<String,String> producer;

    public Producer(){

    }

    @PostConstruct
    public void init(){
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", brokerList);

        kafkaProps.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks", "1");

        kafkaProps.put("retries", "1");
        kafkaProps.put("linger.ms", 5);

        producer = new KafkaProducer<>(kafkaProps);
    }

    public void send(String value) throws ExecutionException, InterruptedException {
        if (sync.equalsIgnoreCase("sycn")){
            sendSycn(value);
        }else{
            sendAsycn(value);
        }

    }

    private void sendSycn(String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record).get();


    }

    private void sendAsycn(String value){
        ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,value);
        producer.send(record,(RecordMetadata recordMetadata,Exception e)-> {
            if(e!= null){
                e.printStackTrace();
            }
        });

    }


}
