package ge.thesis.sink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/**
 * File sink that writes records without emitting them back to stdout.
 */
public class ResultFileSinkFunction extends RichSinkFunction<String> {

    private final String filePath;
    private final String header;
    private final boolean append;
    private transient BufferedWriter writer;

    public ResultFileSinkFunction(String filePath, String header, boolean append) {
        this.filePath = filePath;
        this.header = header;
        this.append = append;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
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
    public void invoke(String value) throws Exception {
        if (writer != null) {
            writer.write(value);
            writer.newLine();
            writer.flush();
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
        super.close();
    }
}
