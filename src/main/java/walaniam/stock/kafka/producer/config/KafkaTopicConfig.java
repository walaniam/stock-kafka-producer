package walaniam.stock.kafka.producer.config;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapAddress;

    @Value("${stock.kafka.topic}")
    private String topic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = ImmutableMap.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress
        );
        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setAutoCreate(false);
        return admin;
    }

    @Bean
    public NewTopic stockDataTopic() {
        return new NewTopic(topic, 4, (short) 1);
    }
}
