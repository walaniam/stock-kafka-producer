package walaniam.stock.kafka.producer.source;

import lombok.NoArgsConstructor;
import walaniam.stock.kafka.producer.domain.Stock;

import java.util.function.Function;

@NoArgsConstructor(staticName = "of")
public class StockEventKeyFunction implements Function<Stock, String> {
    @Override
    public String apply(Stock stock) {
        return String.format("%s__%s", stock.getTicker(), stock.getTimestamp()).toLowerCase();
    }
}
