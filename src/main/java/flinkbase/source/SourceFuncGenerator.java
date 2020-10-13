package flinkbase.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public interface SourceFuncGenerator {
    public SourceFunction generate();
}
