package flinkbase.utils;

import flinkbase.model.Person;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class FocusUtil {
    private static StreamExecutionEnvironment env;

    public static<T> SingleOutputStreamOperator<T> generateEnableSourceStream(Class<T> tClass, long generateRateMilSecond){
        env = EnvUtil.getLocalWebEnv();
        // 添加source
        SourceFunction<T> source = SourceUtil.createStreamSource(tClass, generateRateMilSecond);

        SingleOutputStreamOperator<T> returns = env.addSource(source).returns(tClass);
        return returns;
    }

    public static<T> SingleOutputStreamOperator<T> generateEnableSourceStreamWithCheckpoint(Class<T> tClass, long generateRateMilSecond){
        env = EnvUtil.getLocalWebEnv();
        EnvUtil.setCheckpoint(env);
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

    public static void start(StreamExecutionEnvironment env, String simple, int durationS) {
        innerStart(env, durationS);
    }

    private static void innerStart(StreamExecutionEnvironment env, int durationS) {
        try {
            JobClient jobClient = env.executeAsync();
            TimeUnit.SECONDS.sleep(durationS);
            jobClient.cancel();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }

    public static void startFoucs(String simple, int durationS) {
        innerStart(env, durationS);
    }
}
