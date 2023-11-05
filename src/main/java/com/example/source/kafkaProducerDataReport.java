package com.example.source;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author: BYDylan
 * @date: 2021/4/22
 * @description: 创建kafka topic的命令
 * bin/kafka-topics.sh  --create --topic auditLog --zookeeper localhost:2181 --partitions 5 --replication-factor 1
 */
public class kafkaProducerDataReport {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "hadoop110:9092");
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        String topic = "auditLog0616";
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        // {"dt":"2018-01-01 10:11:22","type":"shelf","username":"shenhe1","area":"AREA_US"}
        // 生产消息
        for (;;) {
            String message = "{\"dt\":\"" + getCurrentTime() + "\",\"type\":\"" + getRandomType() + "\",\"username\":\"" + getRandomUsername() + "\",\"area\":\"" + getRandomArea() + "\"}";
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(1000);
        }
        // 关闭链接
        // producer.close();
    }

    private static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    private static String getRandomArea() {
        String[] types = {"AREA_US", "AREA_CT", "AREA_AR", "AREA_IN", "AREA_ID"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    private static String getRandomType() {
        String[] types = {"shelf", "unshelf", "black", "chlid_shelf", "child_unshelf"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }


    private static String getRandomUsername() {
        String[] types = {"shenhe1", "shenhe2", "shenhe3", "shenhe4", "shenhe5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
