

## BasicTypeInfo的功能

> package org.apache.flink.api.common.typeinfo;
[查看地址](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/typeinfo/BasicTypeInfo.java)
1. 它继承TypeInformation，所以BTI拥有TypeInformatin的所有基本特性。
2. 同时它是AtomicType， 原子类型，站在语义上来说，它是不可分割的，因此它可以作为独立的key，被划分到各自的partition，因此也是可以比较的。与
原子相对的概念是 composite，也就是复合的意思，复合类型的数据是不能做简单的对比的，那么复合类型中的属于原子类型的数据是可以作为key使用的。

### TypeInfo的基本类型与序列化和比较器的关系
![](./typeinfo.png?raw=true)
从图中可以看到，如果是原子类型的，那么该类型基本也会实现比较器，创建属于自己的比较器

### 案例
我们通过BasicTypeInfo中的IntegerTypeInfo举例

![](./basetypeInfo.svg?raw=true)
```java
public static final BasicTypeInfo<Integer> INT_TYPE_INFO = new IntegerTypeInfo<>(Integer.class, new Class<?>[]{Long.class, Float.class, Double.class, Character.class}, IntSerializer.INSTANCE, IntComparator.class);
```
该变量是存储在BasicTypeInfo类中的静态变量，类型全局只有一份的，flink精简之道：在最大程度上节约内存。

