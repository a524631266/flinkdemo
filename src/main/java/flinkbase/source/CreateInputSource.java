package flinkbase.source;

import flinkbase.utils.EnvUtil;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CreateInputSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
//        InputFormat<?, ?> inputFormat = ;
        Path path = new Path("data/wc.txt");
         // 一次性读取完毕，并直接结束
        TextInputFormat inputFormat = new TextInputFormat(path);
        // 1. 配置文件
//        inputFormat.configure();
//        inputFormat
        DataStreamSource<String> input = env.createInput(inputFormat);

        input.print();
        env.execute("read text");
    }
}
