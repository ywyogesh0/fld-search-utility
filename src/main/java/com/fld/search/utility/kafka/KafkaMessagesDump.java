package com.fld.search.utility.kafka;

import com.cedarsoftware.util.io.JsonWriter;
import com.fld.search.utility.app.MessagesDumpApp;
import com.fld.search.utility.util.DateConversionUtility;
import com.fld.search.utility.model.KafkaTopicParams;
import com.opencsv.CSVReader;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;

import static com.fld.search.utility.util.Constants.*;

public class KafkaMessagesDump implements Runnable {

    private String outputFilePath;
    private String topics;
    private String poll;
    private String dateFormatter;
    private String kafkaCsvPath;
    private String outputFilePathDateFormatter;

    private Set<String> topicsSet = new HashSet<>();
    private Set<KafkaTopicParams> topicsParams = new HashSet<>();
    private List<JSONObject> jsonObjectList = new ArrayList<>();

    private Properties consumerProperties;

    public KafkaMessagesDump(MessagesDumpApp messagesDumpApp) {
        Properties properties = messagesDumpApp.getProperties();

        this.outputFilePath = properties.getProperty(KAFKA_OUTPUT_FILE_PATH);
        this.topics = properties.getProperty(KAFKA_TOPICS);
        this.poll = properties.getProperty(KAFKA_POLL_DURATION_MS);
        this.dateFormatter = properties.getProperty(KAFKA_PARTITION_DATE_FORMATTER);
        this.kafkaCsvPath = properties.getProperty(KAFKA_CSV_PATH);
        this.outputFilePathDateFormatter = properties.getProperty(OUTPUT_FILE_PATH_DATE_FORMATTER);

        String bootstrapServers = properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
        String groupId = properties.getProperty(KAFKA_GROUP_ID);

        // create consumer configs
        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Override
    public void run() {
        try {

            System.out.println("KafkaMessagesDump started...");

            populateTopicsSet();
            populateTopicParams();
            prepareTopicMessagesDump();
            dumpTopicMessages();

            System.out.println("KafkaMessagesDump Finished Successfully...");

        } catch (IOException e) {
            System.out.println("KafkaMessagesDump - ERROR: " + e.getMessage());
        }
    }

    private void populateTopicParams() throws IOException {
        String[] csvLines;
        try (CSVReader csvReader = new CSVReader(new FileReader(kafkaCsvPath))) {
            csvReader.skip(1);
            while ((csvLines = csvReader.readNext()) != null) {
                for (String topic : topicsSet) {
                    topicsParams.add(new KafkaTopicParams(topic, csvLines[0], csvLines[1], csvLines[2]));
                }
            }
        }
    }

    private void populateTopicsSet() {
        String[] topicsArr = topics.split(",");
        if (topicsArr.length > 1) {
            Collections.addAll(topicsSet, topicsArr);
        } else {
            topicsSet.add(topics);
        }
    }

    private void prepareTopicMessagesDump() {
        topicsParams.parallelStream().forEach(topicsParam -> {
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

            String topic = topicsParam.getTopic();
            String key = topicsParam.getKey();
            String offset = topicsParam.getOffset();
            String date = topicsParam.getDate();

            TopicPartition topicPartition = new TopicPartition(topic, partition(topic, key, consumer));
            consumer.assign(Collections.singleton(topicPartition));

            try {
                // filter by offset or timestamp
                if (offset != null && !offset.trim().isEmpty()) {
                    filterByOffSet(consumer, topicPartition, Long.parseLong(offset));
                } else if (date != null && !date.trim().isEmpty()) {
                    filterByTimestamp(consumer, topicPartition,
                            DateConversionUtility.FormattedDateToTimestamp(date, dateFormatter));
                }

                // poll for data
                int count = 0;
                while (count <= 5) {
                    consumer.poll(Duration.ofMillis(Long.parseLong(poll))).forEach(record -> {
                        if (key.equals(record.key())) {

                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("Topic", record.topic())
                                    .put("Partition", record.partition())
                                    .put("Offset", record.offset())
                                    .put("Key", record.key())
                                    .put("Value", record.value())
                                    .put("Timestamp", DateConversionUtility
                                            .timestampToFormattedDate(record.timestamp(), outputFilePathDateFormatter));

                            jsonObjectList.add(jsonObject);
                        }
                    });

                    count += 1;
                }
            } catch (ParseException e) {
                throw new RuntimeException(e.getMessage());
            } finally {
                consumer.close();
            }
        });
    }

    private void filterByOffSet(KafkaConsumer<String, String> consumer, TopicPartition topicPartition, Long offset) {
        consumer.seek(topicPartition, offset);
    }

    private void filterByTimestamp(KafkaConsumer<String, String> consumer, TopicPartition topicPartition, long timestamp) {
        Map<TopicPartition, Long> topicPartitionWithTTimestamps = new HashMap<>();
        topicPartitionWithTTimestamps.put(topicPartition, timestamp);

        consumer.offsetsForTimes(topicPartitionWithTTimestamps).forEach((topicPartitionKey, offsetAndTimestamp) ->
        {
            if (offsetAndTimestamp != null) consumer.seek(topicPartitionKey, offsetAndTimestamp.offset());
        });
    }

    private int partition(String topic, String key, KafkaConsumer consumer) {
        if (key == null) {
            throw new IllegalArgumentException("ERROR : Key is NULL");
        }

        List partitions = consumer.partitionsFor(topic);
        int numPartitions = partitions.size();

        // hash the keyBytes to choose a partition
        return Utils.toPositive(Utils.murmur2(key.getBytes())) % numPartitions;
    }

    private void dumpTopicMessages() throws IOException {
        String filePath = outputFilePath
                + KafkaMessagesDump.class.getSimpleName() + "-"
                + DateConversionUtility.timestampToFormattedDate(System.currentTimeMillis(), outputFilePathDateFormatter)
                + ".json";

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath))) {
            for (JSONObject json : jsonObjectList) {
                bufferedWriter.write(JsonWriter.formatJson(json.toString()));
                bufferedWriter.newLine();
            }
        }
    }
}
