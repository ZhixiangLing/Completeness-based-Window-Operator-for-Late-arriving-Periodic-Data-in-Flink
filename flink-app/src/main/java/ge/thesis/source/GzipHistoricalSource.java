package ge.thesis.source;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Bounded source that streams lines from historical gzip files matching a glob pattern,
 * optionally filtered to a date range (YYYYMMDD strings, inclusive on both ends).
 *
 * Streaming chain per file:
 *   Files.newInputStream(path) -> GZIPInputStream -> InputStreamReader(UTF-8) -> BufferedReader
 *
 * Reads one line at a time and discards it after emitting. Never decompresses a full file
 * to disk and never buffers file content in memory.
 */
public class GzipHistoricalSource extends RichSourceFunction<String> {

    public static final String LINES_ACCUMULATOR = "gzip-input-lines";

    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{8})\\.gz$");
    private static final String PROGRESS_LOG_PATH = "output/results/ingestion_progress.csv";
    private static final String PROGRESS_LOG_HEADER =
            "timestamp,event,linesRead,fileIndex,totalFiles,currentFile,elapsedSec,linesPerSec";

    private final String globPattern;
    private final String fromDate;
    private final String toDate;
    private final String outDir;
    private volatile boolean running = true;
    private transient LongCounter linesCounter;

    public GzipHistoricalSource(String globPattern) {
        this(globPattern, null, null, "output/results");
    }

    public GzipHistoricalSource(String globPattern, String fromDate, String toDate, String outDir) {
        this.globPattern = globPattern;
        this.fromDate = fromDate;
        this.toDate = toDate;
        this.outDir = outDir;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        linesCounter = new LongCounter();
        getRuntimeContext().addAccumulator(LINES_ACCUMULATOR, linesCounter);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        List<Path> files = resolveFiles(globPattern, fromDate, toDate);
        if (files.isEmpty()) {
            throw new IllegalStateException(
                    "No files matched glob: " + globPattern
                    + (fromDate != null || toDate != null
                            ? " (date range: " + fromDate + " - " + toDate + ")"
                            : ""));
        }
        int totalFiles = files.size();
        System.out.printf("[GzipHistoricalSource] Matched %d file(s) | glob: %s | from: %s | to: %s%n",
                totalFiles, globPattern,
                fromDate != null ? fromDate : "*",
                toDate != null ? toDate : "*");

        final long progressIntervalMs = 5L * 60L * 1000L;
        long startMs = System.currentTimeMillis();
        long lastLogMs = startMs;
        long localLines = 0L;
        int fileIndex = 0;

        try (BufferedWriter progressWriter = openProgressWriter()) {
            for (Path file : files) {
                if (!running) {
                    break;
                }
                fileIndex++;
                String fileName = file.getFileName().toString();
                System.out.printf("[GzipHistoricalSource] Reading (%d/%d) %s%n",
                        fileIndex, totalFiles, file);
                writeProgressEvent(progressWriter, "START", localLines, fileIndex, totalFiles,
                        fileName, System.currentTimeMillis() - startMs, rateLinesPerSec(localLines, startMs));

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(file)),
                        StandardCharsets.UTF_8))) {
                    String line;
                    while (running && (line = reader.readLine()) != null) {
                        if (line.isEmpty()) {
                            continue;
                        }
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(line);
                        }
                        linesCounter.add(1L);
                        localLines++;

                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastLogMs >= progressIntervalMs) {
                            long elapsedMs = nowMs - startMs;
                            double rate = rateLinesPerSec(localLines, startMs);
                            System.out.printf(
                                    "[GzipHistoricalSource] Progress: %,d lines | file %d/%d: %s | elapsed %s | rate %,.0f lines/sec%n",
                                    localLines, fileIndex, totalFiles, fileName,
                                    formatElapsed(elapsedMs), rate);
                            writeProgressEvent(progressWriter, "PROGRESS", localLines, fileIndex,
                                    totalFiles, fileName, elapsedMs, rate);
                            lastLogMs = nowMs;
                        }
                    }
                }
                System.out.printf("[GzipHistoricalSource] Finished %s | total so far: %,d lines%n",
                        fileName, localLines);
                writeProgressEvent(progressWriter, "FINISH", localLines, fileIndex, totalFiles,
                        fileName, System.currentTimeMillis() - startMs, rateLinesPerSec(localLines, startMs));
            }
        }
    }

    private BufferedWriter openProgressWriter() throws Exception {
        Path path = Path.of(outDir, "ingestion_progress.csv");
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        boolean writeHeader = !Files.exists(path) || Files.size(path) == 0L;
        BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        if (writeHeader) {
            w.write(PROGRESS_LOG_HEADER);
            w.newLine();
            w.flush();
        }
        return w;
    }

    private static void writeProgressEvent(BufferedWriter w, String event, long linesRead,
                                           int fileIndex, int totalFiles, String currentFile,
                                           long elapsedMs, double linesPerSec) throws Exception {
        w.write(String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%s,%d,%.2f",
                Instant.now(), event, linesRead, fileIndex, totalFiles, currentFile,
                elapsedMs / 1000L, linesPerSec));
        w.newLine();
        w.flush();
    }

    private static double rateLinesPerSec(long lines, long startMs) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        return elapsedMs > 0 ? (lines * 1000.0 / elapsedMs) : 0.0;
    }

    private static String formatElapsed(long ms) {
        long s = ms / 1000;
        long h = s / 3600;
        long m = (s % 3600) / 60;
        long sec = s % 60;
        return String.format("%dh %02dm %02ds", h, m, sec);
    }

    @Override
    public void cancel() {
        running = false;
    }

    static List<Path> resolveFiles(String globPattern, String fromDate, String toDate) throws Exception {
        Path patternPath = Path.of(globPattern);
        Path dir = patternPath.getParent();
        String fileGlob = patternPath.getFileName().toString();
        if (dir == null) {
            dir = Path.of(".");
        }
        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("Directory does not exist: " + dir);
        }
        List<Path> matched = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, fileGlob)) {
            for (Path p : stream) {
                if (Files.isRegularFile(p) && isInDateRange(p, fromDate, toDate)) {
                    matched.add(p);
                }
            }
        }
        Collections.sort(matched);
        return matched;
    }

    private static boolean isInDateRange(Path path, String fromDate, String toDate) {
        if (fromDate == null && toDate == null) {
            return true;
        }
        Matcher m = DATE_PATTERN.matcher(path.getFileName().toString());
        if (!m.find()) {
            return true;
        }
        String date = m.group(1);
        if (fromDate != null && date.compareTo(fromDate) < 0) {
            return false;
        }
        if (toDate != null && date.compareTo(toDate) > 0) {
            return false;
        }
        return true;
    }
}
