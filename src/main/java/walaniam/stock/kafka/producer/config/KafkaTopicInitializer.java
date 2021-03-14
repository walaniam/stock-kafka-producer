package walaniam.stock.kafka.producer.config;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class KafkaTopicInitializer {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private Collection<NewTopic> newTopics;

    @PostConstruct
    public void initialize() {
        log.info("Initializing topics={}", newTopics);
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            newTopics.forEach(topic -> {
                log.info("Creating topic {}", topic);
                CreateTopicsResult topicResult = client.createTopics(ImmutableList.of(topic));
                log.info("Waiting for completion...");
                try {
                    KafkaFuture<Void> allFuture = topicResult.all();
                    allFuture.get(30, TimeUnit.SECONDS);
                    log.info("Completed {}", allFuture);
                } catch (ExecutionException e) {
                    log.warn("Topic not created {}", String.valueOf(e));
                    if (!(e.getCause() instanceof TopicExistsException)) {
                        throw new RuntimeException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
