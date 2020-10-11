package flinkbase.hdfs;

import flinkbase.utils.InOutStreamUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class HadoopSerializationUtilTest {
    private DataInputStream input;
    private DataOutputStream out;

    @Before
    public void init() {
        Tuple2<DataInputViewStreamWrapper, DataOutputViewStreamWrapper> tu = InOutStreamUtil.generatePair();
        input = tu._1;
        out = tu._2;
    }
    @Test
    public void test() throws IOException {

        PersonHD personHD = new PersonHD("zhangll", 20 , "male");
        personHD.write(out);
        PersonHD personHD1 = new PersonHD();
        personHD1.readFields(input);
        // 测试序列化问题
        assertEquals(personHD, personHD1);
    }

    @After
    public void shutdown() throws IOException {
        input.close();
        out.close();
    }
}