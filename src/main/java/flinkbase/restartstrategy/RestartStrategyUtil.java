package flinkbase.restartstrategy;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 故障延时，和故障恢复
 * jobmanager.execution.failover-strategy : full region
 * full 表示所有Task重新故障恢复
 * region  Task会划分为多个Region，这样保证作业的能够在最小集合中选择重启的任务  （Spark中 Taskset-？ ）
 * region是什么 TaskSet，面试过程是这样的。
 */
public class RestartStrategyUtil {
    /**
     * 配置文件参数
     *  这个是默认选择项，因为集群要稳定高效的运行嘛
     * restart-strategy: fixed-delay
     * restart-strategy.fixed-delay.attempts: 5 job在宣告失败之前flink尝试的次数
     * restart-strategy.fixed-delay.delay: 10 s 每次尝试之间的间隔多少
     * @param env
     */
    public static void setRestartStrategy(StreamExecutionEnvironment env) {
        // 固定延时策略 两种方法
//        env.setRestartStrategy(
//                RestartStrategies.fixedDelayRestart(5,
//                        Time.of(10 , java.util.concurrent.TimeUnit.SECONDS)));

        // 1.默认使用 timeout 的值
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5,
                        Time.seconds(10)));
    }
    /**
     * 配置文件参数
     * restart-strategy: failure-rate  故障率重启策略
     * restart-strategy.failure-rate.max-failures-per-interval: 1
     * restart-strategy.failure-rate.failure-rate-interval: 1分钟 表示测量故障率的时间间隔（发起测量的频率）
     * restart-strategy.failure-rate.delay: 1分钟 故障出现之后的延迟时间
     * @param env
     */
    public static void setRestartStrategyByFailureRate(StreamExecutionEnvironment env) {
        // 1. 默认使用 timeout 的值
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(1,
                        Time.of(1, TimeUnit.MINUTES),
                        Time.seconds(10)));

    }

    /**
     * restart-strategy： none
     * 不用重启，这个是直接失败
     * @param env
     */
    public static void setRestartStrategyNo(StreamExecutionEnvironment env) {

        env.setRestartStrategy(RestartStrategies.noRestart());
    }

}
