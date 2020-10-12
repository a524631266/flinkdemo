package flinkbase.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SimpleSink<T> extends RichSinkFunction<T> {
    private String x;
}
