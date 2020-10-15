package flinkbase.source;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import flinkbase.restartstrategy.RestartStrategyUtil;
import flinkbase.source.mysql.model.Organization;
import flinkbase.typeinfo.example.protocol.ProtoColType;
import flinkbase.typeinfo.example.protocol.ProtoColTypeInfo;
import flinkbase.utils.EnvUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.mockito.internal.matchers.Or;

import java.sql.*;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * 标准的mysql一次性语义写法,怎么破
 */
public class MySqlCDCStarter implements SourceFuncGenerator{

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
        StreamExecutionEnvironment env = EnvUtil.createDefaultRemote();
        RestartStrategyUtil.setRestartStrategy(env);
//        EnvUtil.setCheckpoint(env , CheckPoint);
        EnvUtil.setCheckpointWithHDFS(env);
        DataStreamSource<Organization> source = env.addSource(new MySqlCDCStarter().generate());
        source.print().setParallelism(1);
        env.execute("consume orginaztion");
    }

    /**
     * 1个问题，如果source是两个并行流，那么会不会重复读取？???
     * 2. 如何保证source的一次精确性语义
     * @return
     */
    @Override
    public SourceFunction<Organization> generate() {
        return new SimpleMysqlSource();
    }

    /**
     * Exception in thread "main" java.lang.IllegalArgumentException: The parallelism of non parallel operator must be 1.
     */
    static class SimpleMysqlSource extends RichSourceFunction<Organization> implements CheckpointedFunction {

        private PreparedStatement preparedStatement = null;
        private Connection connection = null;
        // 保存最进一次Organization
        private int lastOffset = 0;
        private volatile boolean running = true;
        private ListState<Integer> unionState = null;

        private int count = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(MysqlConfiguration.URL, MysqlConfiguration.username, MysqlConfiguration.password);
            preparedStatement = connection.prepareStatement("select * from organization where id > ?");
        }
        @Override
        public void run(SourceFunction.SourceContext<Organization> ctx) throws Exception {
            if(connection == null) {
                throw new RuntimeException("has no connection");
            }
            int id = lastOffset;
            while (running) {
                preparedStatement.setInt(1, id);
                ResultSet resultSet = preparedStatement.executeQuery();
                lastOffset = simpleHandlerResultReturnMax(ctx, resultSet);
                System.out.println("next part");

                // use mybatis?? TODO(使用 结果集映射方法)
            }
        }

        /**
         * simple handle result
         * @param ctx
         * @param resultSet
         * @return 返回max
         * @throws SQLException
         * @throws InstantiationException
         * @throws IllegalAccessException
         */
        private int simpleHandlerResultReturnMax(SourceContext<Organization> ctx, ResultSet resultSet) throws SQLException, InstantiationException, IllegalAccessException, InterruptedException {
            // simple handle result
            int max = 1;
            while (resultSet.next()){
                Organization organization = Organization.class.newInstance();
                int id = resultSet.getInt(1);
                organization.setId(id);
                organization.setName(resultSet.getString(2));
                organization.setCoding(resultSet.getString(3));
                organization.setMemo(resultSet.getString(4));
                organization.setImporttime(resultSet.getLong(5));
                organization.setImportuserid(resultSet.getInt(6));
                organization.setUpdatetime(resultSet.getLong(7));
                organization.setUpdateuserid(resultSet.getInt(8  ));
                organization.setDeletetime( resultSet.getLong(9));
                organization.setDeleteuserid(resultSet.getInt(10  ));
                organization.setFast(resultSet.getByte(11  ));
                organization.setParentid(resultSet.getInt(12  ));
                organization.setDescription(resultSet.getString(13  ));
                organization.setIsfrozen(resultSet.getInt(14  ));
                organization.setSalt(resultSet.getInt(15));
                ctx.collect(organization);
                max = max < id ? id : max;
                TimeUnit.MILLISECONDS.sleep(500);
            }
            return max;
        }

        @SneakyThrows
        @Override
        public void cancel(){
            running = false;
            if(preparedStatement!=null){
                preparedStatement.close();
            }
            if(connection!=null){
                connection.close();
            }
        }

        /**
         * TODO THINK
         * 1. 当List OpertatorState中的数据量状态数据量太大了怎么办
         * 2.
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 用来存储stat的状态，如果失败重启，那么就会获取之前的数据
            OperatorStateStore stateStore = context.getOperatorStateStore();
            // 获取上次获得的state
            ListStateDescriptor<Integer> listState = new ListStateDescriptor<>(
                    "orginazation", Integer.class
            );
            unionState = stateStore.getUnionListState(listState);
            // 恢复offset
            int max = 0;
            Iterator<Integer> iterator = unionState.get().iterator();
            while (iterator.hasNext()){
                Integer next = iterator.next();
                System.out.println("restore id:" + next);
                max = max > next ? max: next;
            }
            System.out.println("watch MAX: " + max);
            lastOffset = max;
        }

        /**
         * 当流进行快照备份的时候，需要设计一个值
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snap " + ++count);
            unionState.add(lastOffset);
        }
    }

}
