package ge.thesis;

import ge.thesis.config.WatermarkFactory;
import ge.thesis.config.WindowConfig;
import ge.thesis.evaluation.DroppedRecordCounter;
import ge.thesis.evaluation.FaultyWindowCountAggregator;
import ge.thesis.evaluation.OutputCountAggregator;
import ge.thesis.evaluation.StandardThroughputMetricsPipeline;
import ge.thesis.evaluation.ThroughputMetricsPipeline;
import ge.thesis.evaluation.WindowFinalizationLatencyEvaluator;
import ge.thesis.model.MeterPhaseLookup;
import ge.thesis.model.MeterReading;
import ge.thesis.model.SmartMeterParser;
import ge.thesis.model.WindowResult;
import ge.thesis.sink.ResultFileSinkFunction;
import ge.thesis.source.GzipHistoricalSource;
import ge.thesis.source.KafkaSourceBuilder;
import ge.thesis.window.AggregationCase1;
import ge.thesis.window.AggregationCase1.MeterPhaseMode;
import ge.thesis.window.Case1AttachWindowInfo;
import ge.thesis.window.NativeAttachWindowInfo;
import ge.thesis.window.trigger.CompletenessSemantic;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class App {
    private static final String MODE_PROPOSED = "proposed";
    private static final String MODE_BASELINE = "baseline";
    private static final String MODE_FAST = "fast";
    private static final String MODE_DELAYED = "delayed";
    private static final Set<String> ALL_MODES = new HashSet<>(Arrays.asList(
            MODE_PROPOSED, MODE_BASELINE, MODE_FAST, MODE_DELAYED
    ));

    private static final String SOURCE_KAFKA = "kafka";
    private static final String SOURCE_GZIP = "gzip";
    private static final String DEFAULT_GZIP_GLOB = "/data/RawData/raw_meter_data.log-*.gz";

    private static final String ACC_PARSED_RECORDS = "parsed-records-count";
    private static final String ACC_RESULT_ROWS = "result-rows-count";

    /** Captures every tuple the window operator filters out as too-late. */
    private static final OutputTag<MeterReading> LATE_OUTPUT_TAG =
            new OutputTag<MeterReading>("late-records") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParsedArgs parsed = parseArgs(args);
        runUseCase1WithCsvLookup(env, parsed);

        JobExecutionResult result = null;
        Throwable executeError = null;
        try {
            result = env.execute(
                    "Smart Meter Use Case 1 " + parsed.mode.toUpperCase() + " Experiment");
        } catch (Throwable t) {
            executeError = t;
        } finally {
            // Always emit a run summary — even if env.execute() threw — so the operator
            // gets a row in run_summary.csv (or at least a [run_summary] log line) for every run.
            printRunSummary(parsed, result);
        }

        if (executeError != null) {
            throw new Exception("Job execution failed", executeError);
        }
    }

    private static void runUseCase1WithCsvLookup(StreamExecutionEnvironment env, ParsedArgs parsed) throws Exception {
        String mode = parsed.mode;
        MeterPhaseLookup meterPhaseLookup = MeterPhaseLookup.fromCsvFiles(
    "/data/ling_zhixiang/meter_classification/single_phase_meters.csv",
    "/data/ling_zhixiang/meter_classification/three_phase_meters.csv"
        );

        System.out.printf(
                "Running mode: %s | Loaded meter phase lookup: %d single-phase meters, %d three-phase meters%n",
                mode,
                meterPhaseLookup.singlePhaseCount(),
                meterPhaseLookup.threePhaseCount()
        );

        if (MODE_FAST.equals(mode)) {
            System.out.printf("[fast mode] ignoring time argument '%s' (strict watermark, zero allowed lateness)%n",
                    parsed.timeSettingStr);
        }

        ThroughputMetricsPipeline throughputMetrics = new ThroughputMetricsPipeline(mode, parsed.outDir);
        StandardThroughputMetricsPipeline standardThroughputMetrics =
                new StandardThroughputMetricsPipeline(mode, parsed.outDir);

        SingleOutputStreamOperator<String> rawLineStream;
        if (SOURCE_GZIP.equals(parsed.source)) {
            rawLineStream = env.addSource(
                    new GzipHistoricalSource(parsed.gzipGlob, parsed.fromDate, parsed.toDate, parsed.outDir),
                    "Gzip Historical Source");
        } else {
            KafkaSource<String> source = KafkaSourceBuilder.build("cm_meter_values");
            rawLineStream = env.fromSource(
                    source,
                    WatermarkStrategy.noWatermarks(),
                    "Kafka Smart Meter Source");
        }
        SingleOutputStreamOperator<String> sourceThroughputStream =
                standardThroughputMetrics.attachSourceProbe(rawLineStream);

        // Mode-aware watermark strategy: only "delayed" uses a non-zero out-of-orderness bound.
        WatermarkStrategy<MeterReading> wmStrategy =
                MODE_DELAYED.equals(mode)
                        ? WatermarkFactory.createDelayedStrategy(parsed.timeSetting)
                        : WatermarkFactory.createBasicStrategy();

        SingleOutputStreamOperator<MeterReading> parsedReadingStream = sourceThroughputStream
                .map(SmartMeterParser::parse)
                .map(new ParsedRecordCounter())
                .assignTimestampsAndWatermarks(wmStrategy);

        SingleOutputStreamOperator<MeterReading> readingStream = throughputMetrics.attachSourceProbe(parsedReadingStream);

        final Set<String> threePhaseVoltageSeries = new HashSet<>(Arrays.asList(
                AggregationCase1.SERIES_L1,
                AggregationCase1.SERIES_L2,
                AggregationCase1.SERIES_L3
        ));

        DataStream<MeterReading> singlePhaseVoltageStream = readingStream
                .filter(reading ->
                        reading.seriesId != null &&
                        AggregationCase1.SERIES_L1.equals(reading.seriesId) &&
                        meterPhaseLookup.isSinglePhase(reading.meterId));

        DataStream<MeterReading> threePhaseVoltageStream = readingStream
                .filter(reading ->
                        reading.seriesId != null &&
                        threePhaseVoltageSeries.contains(reading.seriesId) &&
                        meterPhaseLookup.isThreePhase(reading.meterId));
        DataStream<MeterReading> postFilterThroughputStream =
                singlePhaseVoltageStream.union(threePhaseVoltageStream);

        // Effective allowed lateness: zero for fast/delayed (industrial defaults).
        Duration effectiveLateness =
                (MODE_FAST.equals(mode) || MODE_DELAYED.equals(mode))
                        ? Duration.ZERO
                        : parsed.timeSetting;

        // Build single-phase + three-phase window operators for the active mode.
        // Proposed → CompletenessTrigger; everyone else → BaselineTrigger.
        SingleOutputStreamOperator<WindowResult> singlePhaseWindowOp;
        SingleOutputStreamOperator<WindowResult> threePhaseWindowOp;

        if (MODE_PROPOSED.equals(mode)) {
            CompletenessSemantic singlePhaseSemantic = new CompletenessSemantic(
                    WindowConfig.COMPLETENESS_THRESHOLD,
                    WindowConfig.EXPECTED_SLOTS_PER_WINDOW
            );
            CompletenessSemantic threePhaseSemantic = new CompletenessSemantic(
                    WindowConfig.COMPLETENESS_THRESHOLD,
                    WindowConfig.EXPECTED_SLOTS_PER_WINDOW * 3
            );
            singlePhaseWindowOp = buildProposedStream(
                    singlePhaseVoltageStream,
                    singlePhaseSemantic,
                    MeterPhaseMode.SINGLE_PHASE,
                    AggregationCase1.SERIES_L1,
                    effectiveLateness
            );
            threePhaseWindowOp = buildProposedStream(
                    threePhaseVoltageStream,
                    threePhaseSemantic,
                    MeterPhaseMode.THREE_PHASE,
                    "Spänning L1/L2/L3",
                    effectiveLateness
            );
        } else {
            singlePhaseWindowOp = buildBaselineStream(
                    singlePhaseVoltageStream,
                    MeterPhaseMode.SINGLE_PHASE,
                    AggregationCase1.SERIES_L1,
                    effectiveLateness,
                    mode.toUpperCase(Locale.ROOT)
            );
            threePhaseWindowOp = buildBaselineStream(
                    threePhaseVoltageStream,
                    MeterPhaseMode.THREE_PHASE,
                    "Spänning L1/L2/L3",
                    effectiveLateness,
                    mode.toUpperCase(Locale.ROOT)
            );
        }

        // 1. Collect all active result streams for the current mode.
        DataStream<WindowResult> activeStream = singlePhaseWindowOp.union(threePhaseWindowOp);

        // 1.1 CBW uses the FINAL-only stream; native baselines keep their original emission semantics.
        DataStream<WindowResult> finalStream = activeStream
                .filter(r -> "FINAL".equals(r.resultType));

        // 1.5 Drain the late-data side output into DroppedRecordCounter, which feeds a global
        //     accumulator written to run_summary.csv by printRunSummary at job completion.
        singlePhaseWindowOp.getSideOutput(LATE_OUTPUT_TAG)
                .union(threePhaseWindowOp.getSideOutput(LATE_OUTPUT_TAG))
                .addSink(new DroppedRecordCounter());

        // Shared filename suffix for all per-run output CSVs.
        String fileSuffix = "_" + parsed.runId + "_" + parsed.mode + "_" + parsed.timeSettingStr;

        // 2. finalization_latency_metrics only makes physical sense in proposed mode, where
        //    FIRE_AND_PURGE is the actual window termination. For baseline/fast/delayed the FINAL
        //    rows are only post-watermark update events, not finalization events, so we skip the
        //    file for those modes.
        if (MODE_PROPOSED.equals(mode)) {
            finalStream
                    .map(WindowFinalizationLatencyEvaluator::csvRow)
                    .map(new ResultRowCounter())
                    .addSink(new ResultFileSinkFunction(
                            parsed.outDir + "/finalization_latency_metrics" + fileSuffix + ".csv",
                            "mode,resultType,finalizationReason,meterId,seriesId,count,faultIndicator,windowStartMs,windowEndMs,watermarkAtPurgeMs,allowedLatenessMs,finalizationLatencyMs,latencyReductionMs",
                            false
                    ));
        }

        // 2.5 Output-count + early-purge ratio + final count: one row per (meter, series, window),
        //     no per-tuple records. finalCount is the canonical source for offline
        //     output-completeness analysis across all four modes.
        activeStream
                .keyBy(r -> r.meterId + "|" + r.seriesId + "|" + r.windowStartMs)
                .process(new OutputCountAggregator(effectiveLateness.toMillis()))
                .addSink(new ResultFileSinkFunction(
                        parsed.outDir + "/output_counts" + fileSuffix + ".csv",
                        "mode,meterId,seriesId,windowStartMs,windowEndMs,outputCount,earlyPurged,finalCount,distinctSlotCount",
                        false
                ));

        // 3. Business aggregation (faulty-meter counting): CBW counts finalized faulty windows.
        //    Native baselines output every native emission marked faulty; offline analysis decides
        //    whether to use any-emission or last-observed semantics.
        DataStream<WindowResult> faultySourceStream =
                MODE_PROPOSED.equals(mode) ? finalStream : activeStream;
        String faultyFilePrefix = MODE_PROPOSED.equals(mode)
                ? "/faulty_window_counts"
                : "/native_faulty_window_counts";
        faultySourceStream
                .filter(r -> r.faultIndicator)
                .map(new FaultyWindowCountAggregator())
                .addSink(new ResultFileSinkFunction(
                        parsed.outDir + faultyFilePrefix + fileSuffix + ".csv",
                        "meterId,seriesId,windowStart,windowEnd,mode,singlePhaseUniqueFaultyMeterCount,threePhaseUniqueFaultyMeterCount,totalUniqueFaultyMeterCount,voltageData",
                        false
                ));

        // Shared filename suffix for the throughput-metrics outputs.
        String throughputSuffix = "_" + parsed.runId + "_" + parsed.mode + "_" + parsed.timeSettingStr;
        standardThroughputMetrics.writeMetrics(sourceThroughputStream, postFilterThroughputStream, throughputSuffix);
        throughputMetrics.writeMetrics(readingStream, activeStream, throughputSuffix);
    }

    private static SingleOutputStreamOperator<WindowResult> buildProposedStream(
            DataStream<MeterReading> input,
            CompletenessSemantic semantic,
            MeterPhaseMode phaseMode,
            String seriesLabel,
            java.time.Duration allowedLateness
    ) {
        return input
                .keyBy(reading -> reading.meterId)
                .window(TumblingEventTimeWindows.of(WindowConfig.WINDOW_SIZE))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(LATE_OUTPUT_TAG)
                .trigger(new ge.thesis.window.trigger.CompletenessTrigger(semantic, allowedLateness))
                .aggregate(
                        new AggregationCase1(phaseMode, true),
                        new Case1AttachWindowInfo(semantic, "PROPOSED", seriesLabel, allowedLateness)
                );
    }

    /**
     * Builds a window operator that uses the standard {@link ge.thesis.window.trigger.BaselineTrigger}
     * for the given mode. Used by the {@code baseline}, {@code fast} and {@code delayed} modes;
     * the {@code modeLabel} ends up tagged onto every {@link WindowResult} so downstream evaluation
     * pipelines can keep the rows from different modes apart.
     */
    private static SingleOutputStreamOperator<WindowResult> buildBaselineStream(
            DataStream<MeterReading> input,
            MeterPhaseMode phaseMode,
            String seriesLabel,
            java.time.Duration allowedLateness,
            String modeLabel
    ) {
        return input
                .keyBy(reading -> reading.meterId)
                .window(TumblingEventTimeWindows.of(WindowConfig.WINDOW_SIZE))
                .allowedLateness(allowedLateness)
                .sideOutputLateData(LATE_OUTPUT_TAG)
                .trigger(new ge.thesis.window.trigger.BaselineTrigger(allowedLateness))
                .aggregate(
                        new AggregationCase1(phaseMode, false),
                        new NativeAttachWindowInfo(modeLabel, seriesLabel)
                );
    }

    private static ParsedArgs parseArgs(String[] args) {
        // CLI argument order matches run_experiment.sh:
        //   [runId] [mode] [timeSetting] [source] [glob] [fromDate] [toDate] [outDir]
        // timeSetting semantics depend on mode:
        //   proposed/baseline -> allowed lateness
        //   delayed           -> watermark out-of-orderness bound
        //   fast              -> any value, ignored (strict watermark + zero allowed lateness)

        String runId = "default_run";
        if (args.length >= 1) {
            runId = args[0].trim();
        }

        String mode = MODE_PROPOSED;
        if (args.length >= 2) {
            String raw = args[1].trim().toLowerCase();
            if (!ALL_MODES.contains(raw)) {
                throw new IllegalArgumentException("Unsupported mode: " + args[1]
                        + ". Expected one of: " + ALL_MODES);
            }
            mode = raw;
        }

        String timeSettingStr = "50h";
        if (args.length >= 3) {
            timeSettingStr = args[2].trim();
        }
        java.time.Duration timeSetting = parseDuration(timeSettingStr);

        String source = SOURCE_KAFKA;
        if (args.length >= 4) {
            String raw = args[3].trim().toLowerCase();
            if (!SOURCE_KAFKA.equals(raw) && !SOURCE_GZIP.equals(raw)) {
                throw new IllegalArgumentException("Unsupported source: " + args[3]
                        + ". Expected one of: kafka, gzip.");
            }
            source = raw;
        }

        String gzipGlob = DEFAULT_GZIP_GLOB;
        if (args.length >= 5) {
            gzipGlob = args[4].trim();
        }

        String fromDate = null;
        if (args.length >= 6) {
            fromDate = validateDate(args[5], "fromDate");
        }

        String toDate = null;
        if (args.length >= 7) {
            toDate = validateDate(args[6], "toDate");
        }

        String outDir = "output/results";
        if (args.length >= 8) {
            outDir = args[7].trim();
        }

        return new ParsedArgs(runId, mode, source, gzipGlob, fromDate, toDate, outDir, timeSetting, timeSettingStr);
    }

    private static java.time.Duration parseDuration(String raw) {
        if ("0".equals(raw)) {
            return java.time.Duration.ZERO;
        }
        if (raw.endsWith("h")) {
            return java.time.Duration.ofHours(Long.parseLong(raw.substring(0, raw.length() - 1)));
        } else if (raw.endsWith("m")) {
            return java.time.Duration.ofMinutes(Long.parseLong(raw.substring(0, raw.length() - 1)));
        } else if (raw.endsWith("d")) {
            return java.time.Duration.ofDays(Long.parseLong(raw.substring(0, raw.length() - 1)));
        }
        throw new IllegalArgumentException("Unsupported duration format (use 0, Nh, Nm, or Nd): " + raw);
    }

    private static void printRunSummary(ParsedArgs parsed, JobExecutionResult result) {
        boolean jobAborted = result == null;
        long inputLines = jobAborted ? 0L : accumulatorAsLong(result, GzipHistoricalSource.LINES_ACCUMULATOR);
        long parsedRecords = jobAborted ? 0L : accumulatorAsLong(result, ACC_PARSED_RECORDS);
        long resultRows = jobAborted ? 0L : accumulatorAsLong(result, ACC_RESULT_ROWS);
        long droppedRecords = jobAborted ? 0L : accumulatorAsLong(result, DroppedRecordCounter.ACCUMULATOR_NAME);
        double droppedRatio = parsedRecords > 0 ? ((double) droppedRecords) / parsedRecords : 0.0;

        String timeSettingLabel;
        String timeSettingDisplay;
        switch (parsed.mode) {
            case MODE_DELAYED:
                timeSettingLabel = "watermark delay          ";
                timeSettingDisplay = parsed.timeSettingStr;
                break;
            case MODE_FAST:
                timeSettingLabel = "time setting             ";
                timeSettingDisplay = "(ignored — fast mode)";
                break;
            default:
                timeSettingLabel = "allowed lateness         ";
                timeSettingDisplay = parsed.timeSettingStr;
                break;
        }
        if (jobAborted) {
            timeSettingDisplay = timeSettingDisplay + "  [job aborted — accumulators unavailable]";
        }

        System.out.println();
        System.out.println("=== Run summary ===");
        System.out.printf("mode                      : %s%n", parsed.mode);
        System.out.printf("source                    : %s%n", parsed.source);
        if (SOURCE_GZIP.equals(parsed.source)) {
            System.out.printf("gzip glob                 : %s%n", parsed.gzipGlob);
            System.out.printf("date from                 : %s%n",
                    parsed.fromDate != null ? parsed.fromDate : "(no filter)");
            System.out.printf("date to                   : %s%n",
                    parsed.toDate != null ? parsed.toDate : "(no filter)");
        }
        System.out.printf("total input lines read    : %d%n", inputLines);
        System.out.printf("total parsed records      : %d%n", parsedRecords);
        System.out.printf("total result rows written : %d%n", resultRows);
        System.out.printf("total dropped records     : %d (%.4f%% of parsed)%n",
                droppedRecords, droppedRatio * 100.0);
        System.out.printf("%s : %s%n", timeSettingLabel.trim(), timeSettingDisplay);
        System.out.printf("output dir                : %s%n", parsed.outDir);

        appendRunSummaryRow(parsed, parsedRecords, droppedRecords, droppedRatio);
    }

    /**
     * Appends one row per Java run to {@code <outDir>/run_summary.csv} so cross-mode comparisons
     * can be done with a single {@code pandas.read_csv}. Header is written only when the file
     * does not yet exist or is empty.
     */
    private static void appendRunSummaryRow(ParsedArgs parsed, long totalInputRecords,
                                            long droppedRecords, double droppedRatio) {
        Path summaryPath = Path.of(parsed.outDir, "run_summary.csv").toAbsolutePath();
        System.out.println("[run_summary] attempting to write to: " + summaryPath);
        try {
            if (summaryPath.getParent() != null) {
                Files.createDirectories(summaryPath.getParent());
            }
            boolean writeHeader = !Files.exists(summaryPath) || Files.size(summaryPath) == 0L;
            try (BufferedWriter w = Files.newBufferedWriter(summaryPath, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                if (writeHeader) {
                    w.write("mode,runId,timeSetting,totalInputRecords,droppedRecords,droppedRecordRatio");
                    w.newLine();
                }
                w.write(String.format(Locale.ROOT, "%s,%s,%s,%d,%d,%.6f",
                        parsed.mode,
                        parsed.runId,
                        parsed.timeSettingStr,
                        totalInputRecords,
                        droppedRecords,
                        droppedRatio));
                w.newLine();
            }
            System.out.println("[run_summary] wrote row to " + summaryPath);
        } catch (Exception e) {
            System.out.println("[run_summary] FAILED to write to " + summaryPath + ": " + e);
            e.printStackTrace(System.out);
        }
    }

    private static String validateDate(String raw, String argName) {
        String d = raw.trim();
        if (!d.matches("\\d{8}")) {
            throw new IllegalArgumentException(
                    argName + " must be 8 digits (YYYYMMDD), got: " + raw);
        }
        return d;
    }

    private static long accumulatorAsLong(JobExecutionResult result, String name) {
        Object value = result.getAccumulatorResult(name);
        if (value instanceof Number n) {
            return n.longValue();
        }
        return 0L;
    }

    private static final class ParsedArgs {
        final String runId;
        final String mode;
        final String source;
        final String gzipGlob;
        final String fromDate;
        final String toDate;
        final String outDir;
        /**
         * args[2] parsed as a Duration. Semantic varies by mode:
         *   proposed/baseline → allowed lateness
         *   delayed           → watermark out-of-orderness bound
         *   fast              → ignored
         */
        final java.time.Duration timeSetting;
        final String timeSettingStr;

        ParsedArgs(String runId, String mode, String source, String gzipGlob, String fromDate, String toDate, String outDir, java.time.Duration timeSetting, String timeSettingStr) {
            this.runId = runId;
            this.mode = mode;
            this.source = source;
            this.gzipGlob = gzipGlob;
            this.fromDate = fromDate;
            this.toDate = toDate;
            this.outDir = outDir;
            this.timeSetting = timeSetting;
            this.timeSettingStr = timeSettingStr;
        }
    }

    public static final class ParsedRecordCounter extends RichMapFunction<MeterReading, MeterReading> {
        private transient LongCounter counter;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            counter = new LongCounter();
            getRuntimeContext().addAccumulator(ACC_PARSED_RECORDS, counter);
        }

        @Override
        public MeterReading map(MeterReading value) {
            counter.add(1L);
            return value;
        }
    }

    public static final class ResultRowCounter extends RichMapFunction<String, String> {
        private transient LongCounter counter;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            counter = new LongCounter();
            getRuntimeContext().addAccumulator(ACC_RESULT_ROWS, counter);
        }

        @Override
        public String map(String value) {
            counter.add(1L);
            return value;
        }
    }

    private static String resolveCsvPath(String path) {
        Path directPath = Path.of(path);
        if (Files.exists(directPath)) {
            return directPath.toString();
        }

        Path parentPath = Path.of("..", path);
        if (Files.exists(parentPath)) {
            return parentPath.toString();
        }

        return directPath.toString();
    }
}
