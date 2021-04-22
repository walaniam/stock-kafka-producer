package walaniam.stock.domain;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import static org.apache.commons.lang3.StringUtils.substringBefore;

@Builder
@Getter
@ToString(of = {"ticker", "date"})
@EqualsAndHashCode
public class Stock {

    private static final String END_PILL = "__END_PILL";

    private final String ticker;
    private final long timestamp;
    private final float openPrice;
    private final float closePrice;
    private final float lowestPrice;
    private final float highestPrice;
    private final float volume;

    public static Stock endPillOf(String ticker) {
        return Stock.builder()
                .ticker(ticker + END_PILL)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    public static boolean isEndPill(Stock stock) {
        return stock.ticker.endsWith(END_PILL);
    }

    public static String endPillTickerOf(Stock stock) {
        if (isEndPill(stock)) {
            return substringBefore(stock.ticker, END_PILL);
        }
        throw new IllegalArgumentException(stock.ticker + " is not an end pill");
    }
}
