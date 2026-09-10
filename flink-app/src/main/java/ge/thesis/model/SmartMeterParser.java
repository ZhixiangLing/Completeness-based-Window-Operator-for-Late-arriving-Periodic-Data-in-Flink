package ge.thesis.model;


import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;

public class SmartMeterParser {

    private static final DateTimeFormatter FLEXIBLE_TS = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
            .optionalEnd()
            .toFormatter();

    public static MeterReading parse(String stream) {
        if (stream == null || stream.isBlank()) {
            throw new IllegalArgumentException("Raw message is empty");
        }

        if (!stream.startsWith("Struct{") || !stream.endsWith("}")) {
            throw new IllegalArgumentException("Unexpected message format: " + stream);
        }

        String body = stream.substring("Struct{".length(), stream.length() - 1);

        String[] parts = body.split(",(?=[a-zA-Z_]+=)");
        Map<String, String> fields = new HashMap<>();

        for (String part : parts) {
            int idx = part.indexOf('=');
            if (idx < 0) {
                continue;
            }

            String key = part.substring(0, idx).trim();
            String value = part.substring(idx + 1).trim();
            fields.put(key, value);
        }

        String seriesId = fields.get("series_id");
        String locationId = fields.get("location_id");
        String meterId = fields.get("meter_id");
        String time = fields.get("time");
        String valueStr = fields.get("value");
        String valueStatus = fields.get("value_status");
        String insertTime = fields.get("insert_time");
        String extractTime = fields.get("extract_time");

        if (seriesId == null || locationId == null || meterId == null ||
                time == null || valueStr == null || valueStatus == null ||
                insertTime == null || extractTime == null) {
            throw new IllegalArgumentException("Missing required fields in message: " + stream);
        }

        long eventT = parseToEpochMillis(time);
        long insertT = parseToEpochMillis(insertTime);
        long extractT = parseToEpochMillis(extractTime);
        double value = Double.parseDouble(valueStr);

        return new MeterReading(
                seriesId,
                locationId,
                meterId,
                eventT,
                value,
                valueStatus,
                insertT,
                extractT
        );
    }

    private static long parseToEpochMillis(String ts) {
        LocalDateTime ldt = LocalDateTime.parse(ts, FLEXIBLE_TS);
        return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}