package flinkbase.utils;

import flinkbase.model.Person;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FocusUtil {
    private static StreamExecutionEnvironment env;

    public static<T> SingleOutputStreamOperator<T> generateEnableSourceStream(Class<T> tClass, long generateRateMilSecond){
        env = EnvUtil.getLocalWebEnv();
        // 添加source
        SourceFunction<T> source = SourceUtil.createStreamSource(tClass, generateRateMilSecond);

        SingleOutputStreamOperator<T> returns = env.addSource(source).returns(tClass);
        return returns;
    }


    public static<T> SingleOutputStreamOperator<T> generateEnableSourceStream(Class<T> tClass, long generateRateMilSecond, String hdfsPath, String label){
        env = EnvUtil.getLocalWebEnv();
        env.registerCachedFile(hdfsPath, label);
        // 添加source
        SourceFunction<T> source = SourceUtil.createStreamSource(tClass, generateRateMilSecond);

        SingleOutputStreamOperator<T> returns = env.addSource(source).returns(tClass);
        return returns;
    }

    public static void execute(String jobName){
        try {
            env.execute(jobName);
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
