package Kafka_Project.service;

import org.springframework.lang.NonNull;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void sendMessage(@NonNull String message, @NonNull String topic) {
        try {
            CompletableFuture<SendResult<String, String>> future = 
                kafkaTemplate.send(topic, message);
            
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("Message sent successfully to topic: {} at offset: {}", 
                        topic, result.getRecordMetadata().offset());
                } else {
                    logger.error("Failed to send message to topic: {}, error: {}", 
                        topic, ex.getMessage(), ex);
                }
            });
        } catch (Exception e) {
            logger.error("Exception while sending message to topic: {}", topic, e);
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }
    
    public void sendMessageSync(@NonNull String message, @NonNull String topic) {
        try {
            SendResult<String, String> result = kafkaTemplate.send(topic, message).get();
            logger.info("Message sent successfully to topic: {} at offset: {}", 
                topic, result.getRecordMetadata().offset());
        } catch (Exception e) {
            logger.error("Failed to send message to topic: {}", topic, e);
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }
}
