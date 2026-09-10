package ge.thesis.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * Map-function-based file writer compatible with the Flink 2.x lifecycle.
 * Used as a pass-through sink that writes each input string to {@code filePath}.
 */
public class ResultFileSink extends RichMapFunction<String, String> {

    private final String filePath;
    private final String header;
    private final boolean append;
    private transient BufferedWriter writer;

    public ResultFileSink(String filePath) {
        this(filePath, null);
    }

    public ResultFileSink(String filePath, String header) {
        this(filePath, header, true);
    }

    public ResultFileSink(String filePath, String header, boolean append) {
        this.filePath = filePath;
        this.header = header;
        this.append = append;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        initWriter();
    }

    private void initWriter() throws Exception {
        File file = new File(filePath);
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        boolean shouldWriteHeader = header != null && (!append || !file.exists() || file.length() == 0);
        writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(file, append),
                StandardCharsets.UTF_8
        ));
        if (shouldWriteHeader) {
            writer.write(header);
            writer.newLine();
            writer.flush();
        }
    }

    @Override
    public String map(String value) throws Exception {
        if (writer != null) {
            writer.write(value);
            writer.newLine();
            writer.flush();
        }
        return value;
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
        super.close();
    }
}
