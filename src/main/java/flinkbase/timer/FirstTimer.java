package flinkbase.timer;

import flinkbase.model.ActiveModel;
import flinkbase.model.Person;
import flinkbase.utils.FocusUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 技术点
 * 源码级别 Task--》 StreamTask --runMailboxLoop--> （StreamTask有且只有一个实例）MailboxProcessor#runMailboxLoop
 *  runMailboxLoop方法是一个总方法，task流中只触发一次，不过内部有个while循环
 *  private boolean runMailboxStep(TaskMailbox localMailbox, MailboxController defaultActionContext) throws Exception {
 *  // 当处理好事件之后，
 * 		if (processMail(localMailbox)) {
 * 	// 才会触发processElement方法
 * 			mailboxDefaultAction.runDefaultAction(defaultActionContext); // lock is acquired inside default action as needed
 * 			return true;
 *                }
 * 		return false;    * 	}
 * 以上推理，定时任务和processElement是在实际过程中是同步执行的，只是在一个MailBox模型
 *
 * 运用mailbox模型可以优化同步流程
 *
 * mailboxDefaultAction的执行方法在 StreamTask中执行的时候就传递了this::processInput方法进去
 *
 * 1. 在Task doRun中有一个这个按钮 invokable.invoke();
 * 2. StreamTask 中的runMailboxLoop#runMailboxLoop 循环引用遍历接收到的事件
 *
 * checkpoint事件以及timer的事件都会转化成分钟成拥有runatable形式的Mail事件对象，用来表示事件
 * 在flink的runMailboxStep 中会优先处理队列中的事件（cp/timer事件）
 * 执行完之后，才会执行流中的processInput方法
 * http://blog.csdn.net/yuchuanchen/article/details/105677408
 */
public class FirstTimer {
    public static void main(String[] args) {
        SingleOutputStreamOperator<ActiveModel> sourceStream =
                FocusUtil.generateEnableSourceStream(ActiveModel.class, 1_000);

        sourceStream.keyBy(a -> a.getUserId())
                .process(new KeyedProcessFunction<String, ActiveModel, Integer>() {
                    private transient ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<Integer> description = new ValueStateDescriptor<>("age-agg", Integer.class);
                        state = getRuntimeContext().getState(description);
                    }

                    @Override
                    public void processElement(ActiveModel value, Context ctx, Collector<Integer> out) throws Exception {
                        System.out.println("state" + state.value());

                        ctx.timerService().registerProcessingTimeTimer(
                                System.currentTimeMillis() + 1000
                        );
                        // 时间时间必须要支持eventtime
//                        ctx.timerService().registerEventTimeTimer(
//                                System.currentTimeMillis() + 1500
//                        );
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
                        System.out.println("一秒后触发onTimer");
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("close!!!!");
                        super.close();
                        state = null;
                    }
                });
        FocusUtil.startFoucs("firstTimer", 1_000_000_00);
    }
}
