package flinkbase.configuse;

import com.mysql.cj.jdbc.MysqlDataSource;
import flinkbase.utils.EnvUtil;
import flinkbase.utils.PrintUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

public class ConfigInfo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        ExecutionConfig config = env.getConfig();
        showExecutionConfig(config);
        System.out.println("#####################");
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        showCheckpointDefaultInfo(checkpointConfig);
        System.out.println("##########轻量级 配置对象###########");
        // 轻量级 配置对象 . 配置全局变量，在计算逻辑的其他部分能够用到
        // 轻量级配置对象可以用于environment作为全局的配置
        // 也可以用于executionConfig的全局配置
        // Environment <>-- Configuration
        // ExecutionConfig <>-- Configuration
        showGloablParameters(config);
    }

    /**
     * {  confData:{asdf=123} }
     * @param config
     */
    private static void showGloablParameters(ExecutionConfig config) {
        Configuration lightConfig = new Configuration();
        lightConfig.set(ConfigOptions.key("asdf").intType().noDefaultValue(), 123);

        lightConfig.setClass("mysql", MysqlDataSource.class);
        config.setGlobalJobParameters(lightConfig);
        ExecutionConfig.GlobalJobParameters globalJobParameters = config.getGlobalJobParameters();
        Map<String, String> stringStringMap = globalJobParameters.toMap();

        PrintUtil.printObjectFields(globalJobParameters);
    }

    /**
     * 32 种配置，需要了解这些配置信息
     *  静态field
     * {  PARALLELISM_AUTO_MAX:2147483647 }
     * {  PARALLELISM_DEFAULT:-1 }
     * {  PARALLELISM_UNKNOWN:-2 }
     * {  DEFAULT_RESTART_DELAY:10000 }
     *
     *  普通field
     * {  executionMode:PIPELINED }
     * {  closureCleanerLevel:RECURSIVE }
     * {  parallelism:4 }
     * {  maxParallelism:-1 }
     * {  numberOfExecutionRetries:-1 }
     * {  forceKryo:false }
     * {  disableGenericTypes:false }
     * {  enableAutoGeneratedUids:true }
     * {  objectReuse:false }
     * {  autoTypeRegistrationEnabled:true }
     * {  forceAvro:false }
     * {  codeAnalysisMode:DISABLE }
     * {  autoWatermarkInterval:0 }
     * {  latencyTrackingInterval:0 }
     * {  isLatencyTrackingConfigured:false }
     * {  executionRetryDelay:10000 }
     * {  restartStrategyConfiguration:Cluster level default restart strategy }
     * {  taskCancellationIntervalMillis:-1 }
     * {  taskCancellationTimeoutMillis:-1 }
     * {  useSnapshotCompression:false }
     * {  failTaskOnCheckpointError:true }
     * {  defaultInputDependencyConstraint:ANY }
     * {  globalJobParameters:org.apache.flink.api.common.ExecutionConfig$GlobalJobParameters@1 }
     * {  registeredTypesWithKryoSerializers:{} }
     * {  registeredTypesWithKryoSerializerClasses:{} }
     * {  defaultKryoSerializers:{} }
     * {  defaultKryoSerializerClasses:{} }
     * {  registeredKryoTypes:[] }
     * {  registeredPojoTypes:[] }
     * @param config
     */
    private static void showExecutionConfig(ExecutionConfig config) {
        PrintUtil.printObjectFields(config);
    }

    /**
     * 对于Flink的使用方来说，只要理解checkpoint配置存储的是跟checkpoint相关的配置信息即可
     * 或者可以通过该配置信息就能修改参数
     * {  DEFAULT_MODE:EXACTLY_ONCE }  默认精确一次的执行配置
     * {  DEFAULT_TIMEOUT:600000 } 默认
     * {  DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS:0 }
     * {  DEFAULT_MAX_CONCURRENT_CHECKPOINTS:1 }
     * {  UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER:-1 }
     * {  checkpointingMode:EXACTLY_ONCE }
     * {  checkpointInterval:-1 }
     * {  checkpointTimeout:600000 }
     * {  minPauseBetweenCheckpoints:0 }
     * {  maxConcurrentCheckpoints:1 }
     * {  forceCheckpointing:false }
     * {  unalignedCheckpointsEnabled:false }
     * {  externalizedCheckpointCleanup:null }
     * {  failOnCheckpointingErrors:true }
     * {  preferCheckpointForRecovery:false }
     * {  tolerableCheckpointFailureNumber:-1 }
     */
    private static void showCheckpointDefaultInfo(CheckpointConfig checkpointConfig) {

//        System.out.println(checkpointConfig);
        PrintUtil.printObjectFields(checkpointConfig);
    }


}
