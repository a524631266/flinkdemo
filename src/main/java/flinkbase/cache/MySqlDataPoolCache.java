package flinkbase.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mysql.cj.jdbc.Driver;
import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import flinkbase.model.Entity;
import flinkbase.source.MysqlConfiguration;
import flinkbase.utils.FocusUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * 查找指定的id所对应的祖父节点
 */
public class MySqlDataPoolCache {


    public static void main(String[] args) {
        SingleOutputStreamOperator<Entity> sourceStream = FocusUtil.generateEnableSourceStream(Entity.class, 500L);

        SingleOutputStreamOperator<Entity> map = sourceStream.map(new RichMapFunction<Entity, Entity>() {

            private ComboPooledDataSource dataSource;
            private Cache<String, Entity> cache;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                cache = CacheBuilder.newBuilder()
                        .expireAfterWrite(1, TimeUnit.MINUTES)
                        // 初始多少容量
                        //
                        .initialCapacity(10_000)
                        .maximumSize(100_000)
                        .build();
                dataSource = new ComboPooledDataSource();
                dataSource.setDriverClass(Driver.class.getName());
                dataSource.setUser(MysqlConfiguration.username);
                dataSource.setJdbcUrl(MysqlConfiguration.URL);
                dataSource.setPassword(MysqlConfiguration.password);
                dataSource.setMaxPoolSize(2);
                dataSource.setInitialPoolSize(1);
            }

            @Override
            public Entity map(Entity value) throws Exception {
                if (cache == null) {
                    System.out.println("nulll");
                    return null;
                }
                System.out.println(cache.size());
                System.out.println(cache);
                // 异步引发问题 这个问题不要再出现了！！！！！
//                Entity entity = cache.get(value.getEntity(), new Callable<Entity>() {
//                    @Override
//                    public Entity call() throws Exception {
//
//                        return value;
//                    }
//                });
                Entity ifPresent = cache.getIfPresent(value.getEntity());
                if (ifPresent == null) {
                    Connection connection = dataSource.getConnection();
                    System.out.println("conn: " + connection);
                    PreparedStatement preparedStatement = connection.prepareStatement("select  orgid from entity where id= " + value.getEntity());
                    ResultSet resultSet = preparedStatement.executeQuery();
                    Entity entity1 = new Entity();
                    if (resultSet.next()) {
                        String orgid = resultSet.getString("orgid");
                        System.out.println("向mysql请求 entityid:" + orgid);
                        PreparedStatement preparedStatement1 = connection.prepareStatement("select parentid from organization where id=" + orgid);
                        ResultSet resultSet2 = preparedStatement1.executeQuery();
                        if (resultSet2.next()) {
                            System.out.println("向mysql请求 orgid:" + orgid);
                            String grandId = resultSet2.getString("parentid");
//                                value.parentId = grandId;

                            entity1.setEntity(value.getEntity());
                            entity1.setParentId(grandId);

                        }
                    }
                    // 使用线程池最重要的是要close，否则其他线程找不到线程！！
                    connection.close();
                    cache.put(entity1.getEntity(), entity1);
                    return entity1;
                }else {
                    return ifPresent;
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
                dataSource.close();
            }
        });
        map.printToErr();
        FocusUtil.execute(MySqlDataPoolCache.class.getName());
    }
}
