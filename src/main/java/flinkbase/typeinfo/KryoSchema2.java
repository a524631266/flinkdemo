package flinkbase.typeinfo;

import flinkbase.source.mysql.model.Organization;
import lombok.SneakyThrows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class KryoSchema2 implements SerializationSchema<Organization>, KeyedSerializationSchema<Organization> {
    final ExecutionConfig config;
    private KryoSerializer serializer;
    private DataOutputView out;

    @Override
    public void open(InitializationContext context) {
        MetricGroup metricGroup = context.getMetricGroup();
        serializer = new KryoSerializer<Organization>(Organization.class, config);
        out = new DataOutputSerializer(100);
    }

    public KryoSchema2(ExecutionConfig config) {
        this.config = config;
    }

    @SneakyThrows
    @Override
    public byte[] serialize(Organization element) {

        serializer.serialize(element, out);
        //
//        byte[] sharedBuffer = ((DataOutputSerializer) target).getSharedBuffer();
        byte[] copyOfBuffer = ((DataOutputSerializer) out).getCopyOfBuffer();
        return copyOfBuffer;
//            return new byte[0];
    }

    /**
     * 自定义key， 根据key自动发送数据到指定的分区
     * @param organization
     * @return
     */
    @SneakyThrows
    @Override
    public byte[] serializeKey(Organization organization) {
        int id = organization.getId();
        IntSerializer.INSTANCE.serialize( id , out);
        byte[] copyOfBuffer = ((DataOutputSerializer) out).getCopyOfBuffer();
        return copyOfBuffer;
    }

    /**
     * value
     * @param organization
     * @return
     */
    @SneakyThrows
    @Override
    public byte[] serializeValue(Organization organization) {
        serializer.serialize(organization, out);
        byte[] copyOfBuffer = ((DataOutputSerializer) out).getCopyOfBuffer();
        return copyOfBuffer;
    }

    @Override
    public String getTargetTopic(Organization organization) {
        return null;
    }
}
