package walaniam.stock.kafka.producer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Utils;
import walaniam.stock.domain.Stock;

import java.util.function.Function;

@RequiredArgsConstructor
public class StockPartitionFunction implements Function<Stock, Integer> {

    private final int partitions;

    @Override
    public Integer apply(Stock stock) {
        String ticker = stock.getTicker();
        return Utils.toPositive(Utils.murmur2(ticker.getBytes())) % partitions;
    }
}
