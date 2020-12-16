package flinkbase.schedule;

import org.apache.flink.runtime.jobgraph.ScheduleMode;

/**
 * 调度模型
 * 三种，eager， lazy， with
 */
public class ScheduleModeNote {
    public static void main(String[] args) {
//        System.out.println(ScheduleMode.values());
        for (ScheduleMode scheduleMode : ScheduleMode.values()) {
            System.out.println(scheduleMode);
        }
    }
}
