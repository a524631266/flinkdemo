package flinkbase.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.stumbleupon.async.Deferred;
import flinkbase.model.Entity;
import flinkbase.utils.FocusUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.hbase.async.*;

//import java.util.Collections;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 异步流 查看
 * @apiNote  https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65870673
 *
 * @see AsyncWaitOperator
 */
public class AsyncHbaseCache {
//    @ToString
//    class Entity{
//        @BasicTokenInfo("/0000000[1-9]/")
//        String entity;
//        @BasicTokenInfo("00000000")
//        String parentId;
//    }
    public static void main(String[] args) {
        // 在一毫秒的时候，会大量获取到数据
        SingleOutputStreamOperator<Entity> sourceStream = FocusUtil.generateEnableSourceStream(
                Entity.class, 1);
        RichAsyncFunction<Entity, Entity> richAsyncFunction = new RichAsyncFunction<Entity, Entity>() {

            private HBaseClient hBaseClient;
            private Cache<String, Entity> cache;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                cache = CacheBuilder.newBuilder()
                        .expireAfterWrite(1, TimeUnit.MINUTES)
                        // 初始多少容量
                        .initialCapacity(10_000)
                        .maximumSize(100_000)
                        .build();
//                hbase:
//                zookeeper:
//                quorum: 192.168.10.61
//                property:
//                clientPort: 2181
                hBaseClient = new HBaseClient("192.168.10.61:2181,192.168.10.63:2181,192.168.10.65:2181,192.168.10.67:2181,192.168.10.69:2181",
                        "/hbase");

                MetricGroup hbase =
                        getRuntimeContext().getMetricGroup().addGroup("hbase");
                getRuntimeContext().getMetricGroup();

            }

            @Override
            public void asyncInvoke(Entity value, ResultFuture<Entity> resultFuture) {
                if (cache == null) {
                    return;
                }
                String entityId = String.format("%08d", Integer.valueOf(value.getEntity()));
                System.out.println(entityId);
                Entity entity = cache.getIfPresent(entityId);
                if (entity != null) {
                    resultFuture.complete(Collections.singletonList(entity));
                }
                Scanner entityScanner = hBaseClient.newScanner("entity");
                entityScanner.setFamily("f");
                entityScanner.setStartKey(entityId.getBytes());
                entityScanner.setMaxNumRows(1);

                Deferred<ArrayList<ArrayList<KeyValue>>> arrays = entityScanner.nextRows();
                arrays.addCallback(cb ->{
                    if(!cb.isEmpty()){
//                        System.out.println("获取到！！！" + cb);
//                    return null;
                        Entity entity2 = new Entity();
                        ArrayList<KeyValue> keyValues = cb.get(0);
                        String parentid = "00000000";

                        for (KeyValue keyValue : keyValues) {
                            if (Bytes.wrap(keyValue.qualifier()).toStringUtf8().equals("Parentid")){
                                parentid = Bytes.wrap(keyValue.value()).toStringUtf8();
                            };

                        }
                        entity2.setParentId(parentid);
                        entity2.setEntity(value.getEntity());
                        resultFuture.complete(Collections.singleton(entity2));

                    }
                    return null;
                });

//                hBaseClient.get(new GetRequest("entity".getBytes(),entityId.getBytes(), "f".getBytes(),"Parentid".getBytes()));
//                        .addCallback(cb -> {
//                            List<Entity> objects = Collections.emptyList();
//                            System.out.println("start add CallBack----------------------");
//                            for (KeyValue keyValue : cb) {
//                                String key = String.valueOf(keyValue.key());
//                                String value2 = String.valueOf(keyValue.value());
//                                System.out.println(key+ ":" + value2);
//                                Entity entity1 = new Entity();
//                                entity1.setEntity(key);
//                                entity1.setEntity(value2);
//                                objects.add(entity1);
//                            }
//                            resultFuture.complete(objects);
//                            System.out.println("end add CallBack----------------------");
//                            return null;
//                        });
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println(" close the hbase client");
                hBaseClient.shutdown();
            }
        };
        // 异步流是否用方法,需要等待无序还是有序？
        // 有序代表watermark水印保证数据的有序性。（根据应用场景来判断下游需要有序，还是无序的数据作为依据）
        SingleOutputStreamOperator<Entity> map = AsyncDataStream.
                orderedWait(sourceStream, richAsyncFunction, 1000, TimeUnit.SECONDS,3000);
        map.printToErr();
        FocusUtil.execute(AsyncHbaseCache.class.getName());
    }

}
