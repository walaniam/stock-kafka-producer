package walaniam.stock.kafka.producer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import walaniam.stock.domain.Stock;
import walaniam.stock.kafka.producer.source.CsvStockListingParser;
import walaniam.stock.kafka.producer.source.EventsSender;
import walaniam.stock.kafka.producer.source.FileLoader;
import walaniam.stock.kafka.producer.source.StockEventKeyFunction;

import java.io.File;

@Configuration
public class AppConfig {

    @Value("${stock.kafka.topic}")
    private String eventsTopic;

    @Bean
    public EventsSender<Stock> stockEventsSender(KafkaTemplate<String, Stock> template) {
        return EventsSender.of(template, eventsTopic, StockEventKeyFunction.of());
    }

    @Bean
    public CsvStockListingParser stockListingParser(EventsSender<Stock> sender) {
        return CsvStockListingParser.of(sender);
    }

    @Bean
    public FileLoader fileLoader(CsvStockListingParser parser, @Value("${stock.listings.dir}") String dir) {
        return new FileLoader(parser, new File(dir));
    }
}
