package flinkbase.timer;

import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;

/**
 * 通过该KeyGroup，一个Task（也就是并行任务中的一个独立运行的function任务）拥有一个KeyGroup
 *  结构 为[keyGroup, keyGroup, keyGroup] 以range（坐标id的方式展开）
 *  默认 range 值为32
 *  有start和 end标记 ，表示一个range
 * @see HeapPriorityQueueSet#deduplicationMapsByKeyGroup
 */
public class TimerKeyGroup {
}
