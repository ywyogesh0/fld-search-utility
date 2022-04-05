package com.fld.search.utility.model;

import java.util.Objects;

public class KafkaTopicParams {

    private String topic;
    private String key;
    private String offset;
    private String date;

    public KafkaTopicParams(String topic, String key, String offset, String date) {
        this.topic = topic;
        this.key = key;
        this.offset = offset;
        this.date = date;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getOffset() {
        return offset;
    }

    public String getDate() {
        return date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KafkaTopicParams)) return false;
        KafkaTopicParams that = (KafkaTopicParams) o;
        return getTopic().equals(that.getTopic()) &&
                getKey().equals(that.getKey()) &&
                getOffset().equals(that.getOffset()) &&
                getDate().equals(that.getDate());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTopic(), getKey(), getOffset(), getDate());
    }

    @Override
    public String toString() {
        return "KafkaTopicParams{" +
                "topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", offset='" + offset + '\'' +
                ", date='" + date + '\'' +
                '}';
    }
}
