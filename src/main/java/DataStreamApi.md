Flink Api 逻辑层次
1. Table api -> dataStream Api -表示> transformation api -逻辑执行程序> jobGraph(DAG)

2. Table api &dataStream Api （平行化？？有什么好处）-> transformation api -> jobGraph(DAG)


## DataStream 转换操作



DataStream - keyBy> KeyedStream  -window> WindowedStream  -apply> DataStream

KeyedStream -reduce/aggregation/join(KeyedStream)>   DataStream

DataStream   -windowAll> AllWindowedStream -apply> DataStream

DataStream -connect(DataStream)> ConnectionStream -coMap> DataStream

DataStream -map/filter/union/join> SplitStream -select> DataStream


每个transformation 都需要有一个(ProcessFunction/CoProcessFunction) 实现所有转换操作


## 数据分区
shuffle 洗牌（分区/花色/大小）

上游与下游， 上游计算会通过一条边流到下游

### 分区策略
dataStream.keyBy() 按键值分区，逻辑
dataStream.global() 全部房网第一个实例
dataStream.broadcast(); 广播
dataStream.forward(); 上下游并行读一样时，一对一发送
dataStream.shuffle(); 随机分配
dataStream.rebalance(); round-robin select（轮流分配）
dataStream.rescale(); 本地 rr， 上下游本地性问题
dataStream.partitionCustom(); 自定义单薄


### FLink 连接器
Source + Sink

CSV 持续感知文件变化？　静态数据源（有限）　＋　动态数据源（持续）

官方文档．　比如还有

新版本　已经更新了．
table/sql承担更多的任务（比ｄａｔａｓｔｒｅａｍ　ａｐｉ）１．伪次下退／投影下推？　２．　ｓｃｈｅｍａ定义



