package walaniam.stock.kafka.producer.source;

import lombok.AllArgsConstructor;

import javax.annotation.PostConstruct;
import java.io.File;
import java.util.Arrays;

public class FileLoader {

    private final CsvStockListingParser parser;
    private final File dir;

    public FileLoader(CsvStockListingParser parser, File dir) {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }
        this.parser = parser;
        this.dir = dir;
    }

    @PostConstruct
    public void schedule() {
        Arrays.stream(dir.listFiles()).forEach(parser::parse);
    }

}
