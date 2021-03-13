package walaniam.stock.kafka.producer.source;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import walaniam.stock.kafka.producer.domain.Stock;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

@RequiredArgsConstructor(staticName = "of")
@Slf4j
public class CsvStockListingParser {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final Consumer<Stock> consumer;

    public void parse(InputStream content, String encoding) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(content, encoding))) {
            parseListing(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void parse(File content, String encoding) {
        try (FileInputStream input = new FileInputStream(content)) {
            parse(input, encoding);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void parseListing(BufferedReader reader) throws IOException {
        String line;
        int lineCount = 0;
        while ((line = reader.readLine()) != null) {
            if (++lineCount == 1) {
                continue;
            }
            try {
                String[] columns = line.split(",");
                Stock stock = Stock.builder()
                        .ticker(columns[0].trim().intern())
                        .timestamp(parseDate(columns[1]))
                        .openPrice(Float.parseFloat(columns[2].trim()))
                        .highestPrice(Float.parseFloat(columns[3].trim()))
                        .lowestPrice(Float.parseFloat(columns[4].trim()))
                        .closePrice(Float.parseFloat(columns[5].trim()))
                        .volume(Float.parseFloat(columns[6].trim()))
                        .build();

                consumer.accept(stock);

            } catch (ArrayIndexOutOfBoundsException e) {
                log.warn("Failed to parse line '" + line + "' " + e);
            } catch (Exception e) {
                log.warn("Failed to parse line '" + line + "' " + e);
            }
        }
    }

    private long parseDate(String column) {
        return LocalDateTime.parse(column, formatter)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }
}
