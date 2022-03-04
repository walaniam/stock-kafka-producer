package walaniam.stock.kafka.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Value("${stock.kafka.topic}")
    private String topic;

    @Value("${stock.kafka.partitions}")
    private int partitions;

    @Bean
    public NewTopic stockDataTopic() {
        return new NewTopic(topic, partitions, (short) 1);
    }
}
