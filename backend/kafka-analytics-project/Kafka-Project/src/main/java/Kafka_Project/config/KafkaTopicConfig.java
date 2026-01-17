package Kafka_Project.config;

import java.util.List;
import org.springframework.kafka.core.KafkaAdmin;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    private static final List<String> TOPIC_NAMES = List.of(
        "page_load", "page_view", "link_click", "button_click", "mouse_click",
        "mouse_move", "scroll_depth", "form_submit", "form_focus", "form_input",
        "video_Events", "periodic_events", "page_hidden", "page_unload", "product_view",
        "cart_add", "cart_remove", "purchase", "checkout_step", "custom_event",
        "file_download", "page_visible"
    );

    @Bean
    public KafkaAdmin.NewTopics createTopics() {
        
        return new KafkaAdmin.NewTopics(
            TOPIC_NAMES.stream()
                .map(name -> TopicBuilder.name(name)
                    .partitions(3)
                    .replicas(3)
                    .config("min.insync.replicas", "2")
                    .config("retention.ms", "604800000")
                    .build())
                .toArray(NewTopic[]::new)
        );
    }
}   