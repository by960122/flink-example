package com.example.source;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class kafkaProducer {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "hadoop110:9092");
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        String topic = "allData0615";
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        // {"dt":"2018-01-01 10:11:11","countryCode":"US","data":[{"type":"s1","score":0.3,"level":"A"},{"type":"s2","score":0.2,"level":"B"}]}
        // 生产消息
        while (true) {
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"countryCode\":\"" + getCountryCode() + "\",\"data\":[{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"},{\"type\":\"" + getRandomType() + "\",\"score\":" + getRandomScore() + ",\"level\":\"" + getRandomLevel() + "\"}]}";
            System.out.println(message);
            producer.send(new ProducerRecord<>(topic, message));
            Thread.sleep(2000);
        }
        // producer.close();
    }

    private static String getCurrentTime() {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.now().format(format);
    }

    private static String getCountryCode() {
        String[] types = {"US", "TW", "HK", "PK", "KW", "SA", "IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getRandomType() {
        String[] types = {"s1", "s2", "s3", "s4", "s5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static double getRandomScore() {
        double[] types = {0.3, 0.2, 0.1, 0.5, 0.8};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    private static String getRandomLevel() {
        String[] types = {"A", "A+", "B", "C", "D"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


}
