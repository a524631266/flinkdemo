package flinkbase.typeinfo;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import flinkbase.source.mysql.model.Organization;
import lombok.SneakyThrows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class KyroSchema implements DeserializationSchema<Organization>, SerializationSchema<Organization> {
    final ExecutionConfig config;
    private KryoSerializer serializer;

    public KyroSchema(ExecutionConfig config) {
        this.config = config;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        MetricGroup metricGroup = context.getMetricGroup();
        serializer = new KryoSerializer<Organization>(Organization.class, config);
    }

    @Override
    public Organization deserialize(byte[] message) throws IOException {

        InputStream in2 = new ByteBufferInput(message);
        InputStream in = new BufferedInputStream(in2);
        DataInputView dataView = new DataInputViewStreamWrapper(in);
        KryoSerializer<Organization> ser = new KryoSerializer<>(Organization.class, config);
        Organization result = ser.deserialize(dataView);
        return result;
    }

    @Override
    public boolean isEndOfStream(Organization nextElement) {
        return false;
    }

    @SneakyThrows
    @Override
    public byte[] serialize(Organization element) {
        DataOutputView target = new DataOutputSerializer(100);
        serializer.serialize(element, target);
        //
//        byte[] sharedBuffer = ((DataOutputSerializer) target).getSharedBuffer();
        byte[] copyOfBuffer = ((DataOutputSerializer) target).getCopyOfBuffer();
        return copyOfBuffer;
    }

    @Override
    public TypeInformation<Organization> getProducedType() {
        return TypeInformation.of(Organization.class);
    }
}
