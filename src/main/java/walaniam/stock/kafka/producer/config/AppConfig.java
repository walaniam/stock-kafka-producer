package walaniam.stock.kafka.producer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import walaniam.stock.domain.Stock;
import walaniam.stock.kafka.producer.StockPartitionFunction;
import walaniam.stock.kafka.producer.source.CsvStockListingParser;
import walaniam.stock.kafka.producer.source.EventsSender;
import walaniam.stock.kafka.producer.source.FileLoader;
import walaniam.stock.kafka.producer.source.StockEventKeyFunction;

import java.io.File;

@Configuration
public class AppConfig {

    @Value("${stock.kafka.topic}")
    private String eventsTopic;

    @Value("${stock.kafka.partitions}")
    private int partitions;

    @Bean
    public StockPartitionFunction stockPartitionFunction() {
        return new StockPartitionFunction(partitions);
    }

    @Bean
    public EventsSender<Stock> stockEventsSender(KafkaTemplate<String, Stock> template,
                                                 StockPartitionFunction stockPartitionFunction) {
        return EventsSender.of(template, eventsTopic, StockEventKeyFunction.of(), stockPartitionFunction);
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
