package flinkbase.cache;

import com.google.common.cache.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CacheDemo {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

//        testExpireAfterWrite();
        // 设置之最大存储 size驱逐策略
//        setMaximum();
        // 访问清除策略
//        testVisitStratage();
        // 弱引用: 一旦gc就直接回收
//        weakRefTest();
        // 手动清除元素
//        manuClear();

        // 添加监听函数
//        listenerMethod();

        // callable 缓存
        Cache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(3)
                .build();
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4");
        // 该方法是通过触发callabe返回的数据
        // 一旦没有数据就会触发 callable，有数据不触发！！！！
        String key1 = cache.get("key1", new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println("call");
                return "value1";
            }
        });

        String key4 = cache.get("key4", new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println("call ");
                return "value1";
            }
        });
        System.out.println(cache);
    }

    private static void listenerMethod() {
        RemovalListener<String, String> listener = new RemovalListener<String, String>() {
            @Override
            public void onRemoval(RemovalNotification<String, String> removalNotification) {
                RemovalCause cause = removalNotification.getCause();
                System.out.println(cause);
                // 会触发别驱逐的元素
                System.out.println(String.format("remove: key :%s, value: %s", removalNotification.getKey(), removalNotification.getValue()));
            }
        };
        Cache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(3)
                .removalListener(listener)
                .build();
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4");
    }

    private static void manuClear() {
        Cache<Object, Object> cache = CacheBuilder.newBuilder()
                .build();
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value4");

        List<String> list = new ArrayList<>();
        list.add("key2");
        list.add("key4");
        cache.invalidateAll(list);

        System.out.println(cache.getIfPresent("key4"));
        System.out.println(cache.getIfPresent("key2"));
    }

    /**
     * 弱引用: 一旦gc就直接回收
     * @throws InterruptedException
     */
    private static void weakRefTest() throws InterruptedException {
        Cache<String, Object> cache = CacheBuilder.newBuilder()
                // 当三秒钟之后就过期
                .expireAfterWrite(5, TimeUnit.DAYS)
                // 多久没有访问会过期
                .expireAfterAccess(2, TimeUnit.DAYS)
                // 设置最大的行数，元素数量
                .maximumSize(100_000)
                // 弱引用，用来表示值是弱引用的
                .weakValues()
                .build();

        Object object = new Object();
        cache.put("key1", object);
        object = new Object();
        System.gc();
        TimeUnit.SECONDS.sleep(1);
        System.out.println(cache.getIfPresent("key2"));
    }

    /**
     * 一旦getIfPresent 就会把数据重新放置到
     * @throws InterruptedException
     */
    private static void testVisitStratage() throws InterruptedException {
        Cache<String, String> cache = CacheBuilder.newBuilder()
                // 当三秒钟之后就过期
                .expireAfterWrite(5, TimeUnit.SECONDS)
                // 多久没有访问会过期
                .expireAfterAccess(2, TimeUnit.SECONDS)
                // 设置最大的行数，元素数量
                .maximumSize(2)
                .build();
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.getIfPresent("key1");
        cache.put("key3", "value3");
        cache.getIfPresent("key1");
        cache.put("key4", "value4");
        while (true) {
            String key1 = cache.getIfPresent("key1");
            // 此时 key1 key2 为null
            System.out.println(key1);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    private static void setMaximum() throws InterruptedException {
        Cache<String, String> cache = CacheBuilder.newBuilder()
                // 当三秒钟之后 一定会过期，即使访问成功了
                .expireAfterWrite(3, TimeUnit.DAYS)
                // 设置最大的行数，元素数量
                .maximumSize(2)
                .build();
        cache.put("key1", "value1");
        cache.put("key2", "value2");
        cache.put("key3", "value3");
        cache.put("key4", "value3");
        // 驱逐策略为 先进先出
        while (true) {
            String key1 = cache.getIfPresent("key1");
            // 此时 key1 key2 为null
            System.out.println(key1);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    /**
     * 3秒中之后就直接为null！！！
     * @throws InterruptedException
     */
    private static void testExpireAfterWrite() throws InterruptedException {
        Cache<String, String> cache = CacheBuilder.newBuilder()
                // 当三秒钟之后就过期
                .expireAfterWrite(3, TimeUnit.SECONDS)
                .build();
        cache.put("key1", "value1");

        while (true) {
            String key1 = cache.getIfPresent("key1");
            System.out.println(key1);
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
