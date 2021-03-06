package flinkbase.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import flinkbase.source.MysqlConfiguration;
import flinkbase.utils.FocusUtil;
import lombok.ToString;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.sql.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * 查找指定的id所对应的祖父节点
 */
public class MySqlCache {
    @ToString
    class Entity{
//        @BasicTokenInfo("/0000000[1-9]/")
        @BasicTokenInfo("/[1-9]/")
        String entity;
        @BasicTokenInfo("00000000")
        String parentId;
    }

    public static void main(String[] args) {
        SingleOutputStreamOperator<Entity> sourceStream = FocusUtil.generateEnableSourceStream(Entity.class, 500L);

        SingleOutputStreamOperator<Entity> map = sourceStream.map(new RichMapFunction<Entity, Entity>() {

            private Connection connection;
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
                connection = DriverManager.getConnection(MysqlConfiguration.URL, MysqlConfiguration.username, MysqlConfiguration.password);
            }

            @Override
            public Entity map(Entity value) throws Exception {
                if (cache == null) {
                    return null;
                }
                Entity entity = cache.get(value.entity, new Callable<Entity>() {
                    @Override
                    public Entity call() throws Exception {
                        PreparedStatement preparedStatement = connection.prepareStatement("select  orgid from entity where id= " + value.entity);
                        ResultSet resultSet = preparedStatement.executeQuery();
                        if (resultSet.next()) {
                            String orgid = resultSet.getString("orgid");
                            System.out.println("向mysql请求 entityid:"+ orgid);
                            PreparedStatement preparedStatement1 = connection.prepareStatement("select parentid from organization where id=" + orgid);
                            ResultSet resultSet2 = preparedStatement1.executeQuery();
                            if(resultSet2.next()){
                                System.out.println("向mysql请求 orgid:"+ orgid);
                                String grandId = resultSet2.getString("parentid");
                                value.parentId = grandId;
                            }
                        }

                        return value;
                    }
                });
                return entity;
            }
        });

//        sourceStream.printToErr();
        map.print();
        FocusUtil.execute(MySqlCache.class.getName());
    }
}
