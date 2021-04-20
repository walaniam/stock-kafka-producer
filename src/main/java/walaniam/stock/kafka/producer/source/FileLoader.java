package walaniam.stock.kafka.producer.source;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.*;

@Slf4j
public class FileLoader {

    private static final String LOADED_PREFIX = "l_";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final CsvStockListingParser parser;
    private final File workDir;
    private ScheduledFuture scheduledFuture;

    public FileLoader(CsvStockListingParser parser, File workDir) {
        if (!workDir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + workDir);
        }
        this.parser = parser;
        this.workDir = workDir;
    }

    @PostConstruct
    public synchronized void schedule() {
        if (scheduledFuture != null) {
            throw new UnsupportedOperationException("Already scheduled");
        }
        scheduledFuture = scheduler.scheduleAtFixedRate(this::parseAll, 5, 15, TimeUnit.SECONDS);
    }

    private void parseAll() {
        log.debug("Loading files from {}", workDir);
        CompletableFuture[] parseFutures = Arrays.stream(listNotLoadedFiles())
                .map(file -> CompletableFuture.runAsync(() -> parseAndRename(file)))
                .toArray(size -> new CompletableFuture[size]);
        CompletableFuture.allOf(parseFutures).join();
    }

    private File[] listNotLoadedFiles() {
        File[] files = workDir.listFiles(file -> file.isFile() && !file.getName().startsWith(LOADED_PREFIX));
        log.debug("Loading {} files", files.length);
        return files;
    }

    private void parseAndRename(File file) {
        File newFilename = new File(
                file.getParent(),
                LOADED_PREFIX + System.currentTimeMillis() + "_" + file.getName()
        );
        parser.parse(file);
        boolean renamed = file.renameTo(newFilename);
        log.info("{} moved={} to {}", file.getName(), renamed, newFilename.getName());
    }

    @PreDestroy
    public synchronized void unschedule() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

}
