package walaniam.stock.kafka.producer.source;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.function.Consumer;
import java.util.function.Function;

@AllArgsConstructor(staticName = "of")
@Slf4j
public class EventsSender<T> implements Consumer<T> {

    private final KafkaTemplate<String, T> kafkaTemplate;
    private String eventsTopic;
    private final Function<T, String> keyFunction;

    @Override
    public void accept(T event) {
        String key = keyFunction.apply(event);
        log.debug("Sending {}", key);
        kafkaTemplate.send(eventsTopic, key, event);
    }
}
