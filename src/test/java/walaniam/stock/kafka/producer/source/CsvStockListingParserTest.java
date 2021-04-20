package walaniam.stock.kafka.producer.source;


import org.junit.jupiter.api.Test;
import walaniam.stock.domain.Stock;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvStockListingParserTest {

    private static final String DATA = "<TICKER>,<DTYYYYMMDD>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n" +
            "ALIOR,20121214,59.5000,61.8000,59.5000,60.9000,3035630\n" +
            "ALIOR,20121217,60.0500,61.5000,60.0500,61.2500,206878\n" +
            "ALIOR,20121218,61.8000,62.4000,61.5000,62.2500,390101";

    @Test
    public void parse() {

        final List<Stock> stocks = new ArrayList<>();
        CsvStockListingParser underTest = CsvStockListingParser.of(stocks::add);

        underTest.parse(new ByteArrayInputStream(DATA.getBytes(StandardCharsets.UTF_8)));

        assertThat(stocks).hasSize(3);

        Stock last = stocks.stream()
                .reduce((a, b) -> b)
                .orElseThrow();

        assertThat(last).isEqualTo(
                Stock.builder()
                        .ticker("ALIOR")
                        .timestamp(1355788800000L)
                        .openPrice(61.8f)
                        .highestPrice(62.4f)
                        .lowestPrice(61.5f)
                        .closePrice(62.25f)
                        .volume(390101)
                        .build()
        );
    }
}