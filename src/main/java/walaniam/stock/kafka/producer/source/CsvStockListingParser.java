package walaniam.stock.kafka.producer.source;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import walaniam.stock.domain.Stock;

import java.io.*;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

@RequiredArgsConstructor(staticName = "of")
@Slf4j
public class CsvStockListingParser {

    private static final String ENCODING = "utf-8";

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final Consumer<Stock> consumer;

    public void parse(InputStream content) {
        try (BufferedReader reader = new BufferedReader(readEncoded(content))) {
            parseListing(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void parse(File content) {
        log.info("Parsing {}", content);
        try (FileInputStream input = new FileInputStream(content)) {
            parse(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Reader readEncoded(InputStream input) throws IOException {

        BufferedInputStream bis = new BufferedInputStream(input);
        CharsetDetector detector = new CharsetDetector();
        detector.setText(bis);
        CharsetMatch charsetMatch = detector.detect();

        if (charsetMatch != null) {
            log.debug("Detected charset={}", charsetMatch.getName());
            return charsetMatch.getReader();
        } else {
            log.warn("Could not autodetect charset. Default to {}", ENCODING);
            return new InputStreamReader(input, ENCODING);
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
        return LocalDate.parse(column, formatter)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
    }
}
