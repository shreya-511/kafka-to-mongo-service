

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SyncConnector implements Runnable {
    private static final Logger logger = LogManager.getLogger(SyncConnector.class);
    private final MongoService mongoService;
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper = new ObjectMapper();

    public SyncConnector(MongoService mongoService, Properties props, String SYNC_TOPIC) {
        this.mongoService = mongoService;
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(SYNC_TOPIC));
    }


    @Override
    public void run() {
        while (true) {
            try (Jedis jedis = mongoService.getSentinePool().getResource()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {

                    JsonNode msg = mapper.readTree(record.value());
                    String action = msg.path("action").asText("");
                    JsonNode deviceIds = msg.get("device_id");

                    if (deviceIds != null && deviceIds.isArray()) {

                        for (JsonNode idNode : deviceIds) {
                            String deviceId = idNode.asText();
                            if ("add".equalsIgnoreCase(action)) {
                                mongoService.getMetadataByDeviceId(deviceId).ifPresent(metadata -> {
                                            jedis.set("meta:" + deviceId, metadata.toString());
                                            logger.info("sync :Device {} added to redis  ", deviceId);

                                        }

                                );
                            } else if ("delete".equalsIgnoreCase(action)) {
                                jedis.del("meta:" + deviceId);
                                logger.info("sync :Device {} removed from redis  ", deviceId);
                            }
                        }

                    }
                }
            } catch (Exception e) {
                logger.error("Sync Connector sentinel Error" + e.getMessage());
            }
        }}}

