package flinkbase.source;

import flinkbase.source.mysql.model.Organization;
import flinkbase.typeinfo.example.protocol.ProtoColType;
import flinkbase.typeinfo.example.protocol.ProtoColTypeInfo;
import flinkbase.utils.EnvUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.*;

public class MySqlCDCStarterUserInnerType implements SourceFuncGenerator{
    @Override
    public SourceFunction generate() {
        return null;
    }

//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = EnvUtil.getLocalWebEnv();
//
//        DataStreamSource<ProtoColType<Organization>> source = env.addSource(new MySqlCDCStarterUserInnerType().generate());
//        source.print().setParallelism(1);
//        env.execute("asdft");
//    }
//
//    /**
//     * 1个问题，如果source是两个并行流，那么会不会重复读取？???
//     * 2. 如何保证source的一次精确性语义
//     * @return
//     */
//    @Override
//    public SourceFunction<ProtoColType<Organization>> generate() {
//        return new SimpleMysqlSource();
//    }
//
//    static class SimpleMysqlSource extends RichSourceFunction<ProtoColType<Organization>> implements CheckpointedFunction {
//
//        private PreparedStatement preparedStatement = null;
//        private Connection connection = null;
//        // 保存最进一次Organization
//        private ValueState<ProtoColType<Organization>> state = null;
//
//        private volatile boolean running = true;
//        private ListState<ProtoColType<Organization>> unionState = null;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//
//            Class.forName("com.mysql.jdbc.Driver");
//            connection = DriverManager.getConnection(MysqlConfiguration.URL, MysqlConfiguration.username, MysqlConfiguration.password);
//            preparedStatement = connection.prepareStatement("select * from organization where id > ?");
//        }
//        @Override
//        public void run(SourceContext<ProtoColType<Organization>> ctx) throws Exception {
//            if(connection == null) {
//                throw new RuntimeException("has no connection");
//            }
//            int id = state.value().rawObject.getId();
//            while (running) {
//                if(id == 0){
//                    preparedStatement.setInt(1, id);
//                    ResultSet resultSet = preparedStatement.executeQuery();
//                    simpleHandlerResult(ctx, resultSet);
//                    // use mybatis?? TODO(使用 结果集映射方法)
//                }
//            }
//        }
//
//        /**
//         * simple handle result
//         * @param ctx
//         * @param resultSet
//         * @throws SQLException
//         * @throws InstantiationException
//         * @throws IllegalAccessException
//         */
//        private void simpleHandlerResult(SourceContext<ProtoColType<Organization>> ctx, ResultSet resultSet) throws SQLException, InstantiationException, IllegalAccessException {
//            // simple handle result
//            while (resultSet.next()){
//                Organization organization = Organization.class.newInstance();
//                organization.setId(resultSet.getInt(1));
//                organization.setName(resultSet.getString(2));
//                organization.setCoding(resultSet.getString(3));
//                organization.setMemo(resultSet.getString(4));
//                organization.setImporttime(resultSet.getLong(5));
//                organization.setImportuserid(resultSet.getInt(6));
//                organization.setUpdatetime(resultSet.getLong(7));
//                organization.setUpdateuserid(resultSet.getInt(8  ));
//                organization.setDeletetime( resultSet.getLong(9));
//                organization.setDeleteuserid(resultSet.getInt(10  ));
//                organization.setFast(resultSet.getByte(11  ));
//                organization.setParentid(resultSet.getInt(12  ));
//                organization.setDescription(resultSet.getString(13  ));
//                organization.setIsfrozen(resultSet.getInt(14  ));
//                organization.setSalt(resultSet.getInt(15));
//                ProtoColType<Organization> organizationProtoColType = new ProtoColType<>();
//                organizationProtoColType.rawObject = organization;
//                ctx.collect(organizationProtoColType);
//            }
//            ;
//        }
//
//        @SneakyThrows
//        @Override
//        public void cancel(){
//            running = false;
//            if(preparedStatement!=null){
//                preparedStatement.close();
//            }
//            if(connection!=null){
//                connection.close();
//            }
//        }
//
//        /**
//         * TODO THINK
//         * 1. 当List OpertatorState中
//         *
//         * @param context
//         * @throws Exception
//         */
//        @Override
//        public void initializeState(FunctionInitializationContext context) throws Exception {
//            // 用来存储stat的状态，如果失败重启，那么就会获取之前的数据
//            OperatorStateStore stateStore = context.getOperatorStateStore();
//            Class<? extends ProtoColTypeInfo> aClass = new ProtoColTypeInfo<Organization>(Organization.class).getClass();
//            // 获取上次获得的state
//            Class<ProtoColType<Organization>> ad = ;
//            TypeExtractor.
//            ListStateDescriptor<ProtoColType<Organization>> listState = new ListStateDescriptor<ProtoColType<Organization>>(
//                    "orginazation", TypeExtractor.createTypeInfo(P)
//            );
//            unionState = stateStore.getUnionListState(listState);
////            listState.
//            ValueStateDescriptor<ProtoColType<Organization>> myorg = new ValueStateDescriptor<ProtoColType<Organization>>(
//                    "myorg",
//                    TypeExtractor.getForObject(new ProtoColType<Organization>())
//            );
//
//            state = getRuntimeContext().getState(myorg);
//
//        }
//
//        @Override
//        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//
//        }
//    }

}
