package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.database.DatabaseConnection;
import org.example.serializer.JsonDeserializer;
import org.example.serializer.JsonSerializer;

import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final String TOPIC = "my-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Integer NumRequests = 10000;
    private static final AtomicInteger totalReceived = new AtomicInteger(0);
    private static final AtomicInteger totalSaved = new AtomicInteger(0);
    private static final AtomicBoolean printed = new AtomicBoolean(false);


    public static void main(String[] args) {

    }

    static void produce() {
        long startTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", JsonSerializer.class.getName());
        props.put("batch.size", "32768");
        props.put("linger.ms", 5);
        props.put("acks", "all");
        try (KafkaProducer<String, Message> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < NumRequests; i++) {
                String key = Integer.toString(i);
                String message = "this is message " + Integer.toString(i);
                Message msg = new Message(message, startTime);
                producer.send(new ProducerRecord<String, Message>(TOPIC, key, msg));
            }
        } catch (Exception e) {
            System.out.println("Could not start producer: " + e);
        }
    }

    static void consume() {

        Connection conn = DatabaseConnection.getConnection();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", "my-group-id-test");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", JsonDeserializer.class.getName());
        props.put("json.value.type", Message.class);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("fetch.max.wait.ms", "100");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("fetch.min.bytes", "1");

        List<Message> messages = new ArrayList<>();
        Message lastMessage = null;

        try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(TOPIC));
            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Message> record : records) {
                    messages.add(record.value());
                    lastMessage = record.value();
                    totalReceived.incrementAndGet();
                    if (messages.size() >= 500) {
                        if (saveRecords(conn, messages, lastMessage, consumer)) return;
                    }
                }

                // Xử lý batch cuối: khi poll trả về rỗng nhưng vẫn còn messages chưa được ghi
                if (records.isEmpty() && !messages.isEmpty() && lastMessage != null) {
                    if (saveRecords(conn, messages, lastMessage, consumer)) return;
                }
            }
        } finally {
            DatabaseConnection.closeConnection(conn);
        }
    }

    private static boolean saveRecords(Connection conn, List<Message> messages, Message lastMessage, KafkaConsumer<String, Message> consumer) {
        DatabaseConnection.buckInsertMessages(conn, messages);
        consumer.commitSync();
        int saved = totalSaved.addAndGet(messages.size());
        messages.clear();
        if (saved >= NumRequests &&
                printed.compareAndSet(false, true)) {
            long endTime = System.currentTimeMillis();
            long durationMs = endTime - lastMessage.getCreatedTime();
            double durationSec = durationMs / 1000.0;
            int received = totalReceived.get();
            int lost = NumRequests - saved;
            long tps = Math.round(saved / durationSec);

            System.out.println(String.format("%-16s: %d", "Sent Event",     NumRequests));
            System.out.println(String.format("%-16s: %d", "Received Event", received));
            System.out.println(String.format("%-16s: %d", "Saved Event",    saved));
            System.out.println(String.format("%-16s: %d", "Lost Event",     lost));
            System.out.println(String.format("%-16s: %.1f sec", "Duration", durationSec));
            System.out.println(String.format("%-16s: %d", "TPS",            tps));
            return true;
        }
        return false;
    }

}
