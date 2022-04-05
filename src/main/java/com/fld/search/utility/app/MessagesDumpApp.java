package com.fld.search.utility.app;

import com.fld.search.utility.cassandra.CassandraMessagesDump;
import com.fld.search.utility.kafka.KafkaMessagesDump;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MessagesDumpApp {

    private Properties properties = new Properties();

    public static void main(String[] args) throws IOException {
        MessagesDumpApp messagesDumpApp = new MessagesDumpApp();
        messagesDumpApp.loadPropertyFile(args[0]);

        messagesDumpApp.startKafkaMessagesDump();
        messagesDumpApp.startCassandraMessagesDump();
    }

    private void loadPropertyFile(String propertyFilePath) throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(propertyFilePath)) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void startKafkaMessagesDump() {
        new Thread(new KafkaMessagesDump(this)).start();
    }

    private void startCassandraMessagesDump() {
        new Thread(new CassandraMessagesDump(this)).start();
    }

    public Properties getProperties() {
        return properties;
    }
}
