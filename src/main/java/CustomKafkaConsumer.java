import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.*;

public class CustomKafkaConsumer {

    public static ObjectMapper mapper = new ObjectMapper();
    public static KafkaProducer<String, String> producer = null;
    public static KafkaConsumer<String, String> consumer = null;

    public CustomKafkaConsumer(KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer) {
        this.consumer = consumer;
        this.producer = producer;
    }


    public static final String BOOTSTRAP = System.getenv().getOrDefault("BOOTSTRAP_SERVER", "localhost:9092");
    public static final String CONSUMER_TOPIC = System.getenv().getOrDefault("CONSUMER_TOPIC", "postgres_data");
    public static final String CONSUMER_GROUP = System.getenv().getOrDefault("CONSUMER_GROUP", "json_consumer");
    public static final String SYNC_TOPIC = System.getenv().getOrDefault("SYNC_TOPIC", "sync_action_topic");
    public static final String LIVE_TOPIC = System.getenv().getOrDefault("LIVE_TOPIC", "gps_live_events");
    public static final String HISTORY_TOPIC = System.getenv().getOrDefault("HISTORY_TOPIC", "gps_history_events");
    public static final String SYNC_CONSUMER_GROUP = System.getenv().getOrDefault("SYNC_CONSUMER_GROUP", "sync_data");

    public static final String MONGO_URI = System.getenv().getOrDefault("MONGO_URI", "mongodb://localhost:27017");
    public static final String MONGO_DB = System.getenv().getOrDefault("MONGO_DB_NAME", "fleet_management");
    public static final String MONGO_COLLECT = System.getenv().getOrDefault("MONGO_COLLECTION", "device_metadata");
    public static final String REDIS_MASTER = System.getenv().getOrDefault("REDIS_MASTER", "mymaster");
    public static final String REDIS_SENTINAL_HOST = System.getenv().getOrDefault("REDIS_SENTINAL_HOST", "localhost");
    public static final int REDIS_SENTINAL_PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_SENTINAL_PORT", "26379"));

    public static void main(String[] args) {


        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(CONSUMER_TOPIC));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(producerProps);


        Properties syncProps = new Properties();
        syncProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        syncProps.put(ConsumerConfig.GROUP_ID_CONFIG, SYNC_CONSUMER_GROUP);
        syncProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        syncProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        syncProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        MongoService mongoService = new MongoService();
        mongoService.loadInitialMetadata();
        new Thread(new SyncConnector(mongoService, syncProps, SYNC_TOPIC)).start();



            System.out.println("Consumer Active - Using  Redis Senital mode...");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                try (Jedis jedis = mongoService.getSentinePool().getResource()) {
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            JsonNode packet = mapper.readTree(record.value());
                            mongoService.processStatusBased(packet, jedis, LIVE_TOPIC, HISTORY_TOPIC, 5000L, 60000L);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

}



