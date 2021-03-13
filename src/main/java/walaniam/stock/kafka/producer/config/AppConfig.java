package walaniam.stock.kafka.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import walaniam.stock.kafka.producer.domain.Stock;
import walaniam.stock.kafka.producer.source.CsvStockListingParser;
import walaniam.stock.kafka.producer.source.EventsSender;
import walaniam.stock.kafka.producer.source.StockEventKeyFunction;

@Configuration
public class AppConfig {

    @Bean
    public EventsSender<Stock> stockEventsSender(KafkaTemplate<String, Stock> template) {
        return EventsSender.of(template, StockEventKeyFunction.of());
    }

    @Bean
    public CsvStockListingParser stockListingParser(EventsSender<Stock> sender) {
        return CsvStockListingParser.of(sender);
    }
}
