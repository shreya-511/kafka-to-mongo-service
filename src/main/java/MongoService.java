import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import redis.clients.jedis.Jedis;
import com.mongodb.client.MongoCursor;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;


public class MongoService {
    public final static ObjectMapper mapper = new ObjectMapper();
    private final MongoCollection<Document> collection;
    private final JedisSentinelPool sentinePool;
    private static final Logger logger = LogManager.getLogger(MongoService.class);

    public MongoService() {

        Set<String> sentinels =new HashSet<>();
        sentinels.add(CustomKafkaConsumer.REDIS_SENTINAL_HOST+":"+CustomKafkaConsumer.REDIS_SENTINAL_PORT);
        this.sentinePool =new JedisSentinelPool(CustomKafkaConsumer.REDIS_MASTER,sentinels);
        MongoClient mongoClient = MongoClients.create(CustomKafkaConsumer.MONGO_URI);
        this.collection = mongoClient.getDatabase(CustomKafkaConsumer.MONGO_DB).getCollection(CustomKafkaConsumer.MONGO_COLLECT);
    }
    public JedisSentinelPool getSentinePool(){
        return sentinePool;
    }

    public Optional<JsonNode> getMetadataByDeviceId(String deviceId) {
        Document doc = collection.find(Filters.eq("device_id", deviceId)).first();
        if (doc == null) return Optional.empty();
        try { return Optional.of(mapper.readTree(doc.toJson())); }
        catch (Exception e) { return Optional.empty(); }
    }

    public void loadInitialMetadata() {
        try (Jedis jedis = sentinePool.getResource();
             MongoCursor<Document> cursor = collection.find().iterator()) {
            logger.info("Syncing MongoDB metadata to Redis...");
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                String json = doc.toJson();
                JsonNode metadata = mapper.readTree(json);
                if (metadata.has("device_id")) {
                    jedis.set("meta:" + metadata.get("device_id").asText(), json);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to sync metadata to Redis: ", e);
        }
    }

    public void processStatusBased(JsonNode packet, Jedis jedis, String liveTopic, String historyTopic, Long enrichedInterval, Long xInterval) {
        try {
            String uniqueId = packet.path("uniqueId").asText();
            String metadataJson = jedis.get("meta:" + uniqueId);
            if (metadataJson == null) return;

            JsonNode metadata = mapper.readTree(metadataJson);
            String status = metadata.path("shipment_status").asText("UNKNOWN");
            long interval = ("AP".equalsIgnoreCase(status) || "TP".equalsIgnoreCase(status)) ? enrichedInterval : xInterval;

            long now = System.currentTimeMillis();
            String redisKey = "last_publish_ts:" + uniqueId;
            String lastTsStr = jedis.get(redisKey);
            long lastTs = (lastTsStr != null) ? Long.parseLong(lastTsStr) : 0L;

            if (now - lastTs >= interval) {
                int eventCode = packet.path("packetEventCode").asInt(0);
                String targetTopic = (eventCode <= 100) ? liveTopic : historyTopic;

                ObjectNode enriched = (ObjectNode) packet;
                enriched.setAll((ObjectNode) metadata);
                CustomKafkaConsumer.producer.send(new ProducerRecord<>(targetTopic, uniqueId, enriched.toString()));

                jedis.set(redisKey, String.valueOf(now));
            }
        } catch (Exception e) {
            logger.error("Redis processing error: {}", e.getMessage());
        }
    }

}

