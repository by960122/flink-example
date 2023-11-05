package com.example.source;

import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 实现 kaSerializationSchema
 */
public class KafkaSerializationSchemaDemo<T> implements KafkaSerializationSchema<T>, KafkaContextAware<T> {
    private String topic;

    public KafkaSerializationSchemaDemo(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, Long timestamp) {
        return new ProducerRecord<>(topic, element.toString().getBytes());
    }

    @Override
    public String getTargetTopic(T element) {
        return null;
    }
}
