package flinkbase.utils;


import flinkbase.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * 用来做参数的自动化配置
 */
public class ExecutionEnvUtil {
    /**
     * merge 包括 application.properties中的参数以及传入的参数
     * 优先级
     * 1.系统变量
     * 2.输入参数
     * 3.配置文件
     * @param args
     * @return
     * @throws Exception
     */
    public static ParameterTool createParameterTool(final String[] args) throws Exception {

        return ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    /**
     * 获取默认的
     * @return
     */
    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ParameterTool.fromSystemProperties();
    }

    /**
     * 1. 并行度
     * 2. 重启策略
     * 3. 是否开启checkpoint（如果是，开启的默认间隔）
     * 4. 设置流的时间特性
     * @param parameterTool
     * @return
     * @throws Exception
     */
    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000));
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000));
        }
        env.getConfig().setGlobalJobParameters(parameterTool);
        // TODO 这里默认为EventTime 请注意
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }
}