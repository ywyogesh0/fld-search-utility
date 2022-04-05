package com.fld.search.utility.consumer;

import com.fld.search.utility.util.DateConversionUtility;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "g-t-3";
        String topic = "t-2";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singletonList(topic));

        // poll for new data
        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(1000)); // new in Kafka 2.0.0

        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Topic:" + record.topic());
            System.out.println("Key: " + record.key() + ", Value: " + record.value());
            System.out.println("Timestamp: " + DateConversionUtility.timestampToFormattedDate(record.timestamp(),
                    "yyyy-MM-dd HH:mm:ss.SSS"));
            System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
        }
    }
}
