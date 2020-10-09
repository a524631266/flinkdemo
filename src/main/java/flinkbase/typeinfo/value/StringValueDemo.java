package flinkbase.typeinfo.value;

import flinkbase.utils.InOutStreamUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.StringValue;
import scala.Tuple2;

import java.io.IOException;

import static org.junit.Assert.*;

public class StringValueDemo {
    public static void main(String[] args) throws IOException {
        StringValue a = new StringValue("abacd");
        StringValue b = new StringValue("abacd");
        assertTrue(a.equals(b));
        Tuple2<DataInputViewStreamWrapper, DataOutputViewStreamWrapper> tup = InOutStreamUtil.generatePair();
        for (int i = 0; i < 1000; i++) {
            a.write(tup._2);

            StringValue recieve = new StringValue();
            recieve.read(tup._1);

            assertEquals(a, recieve);
        }
        System.out.println("end");

    }
}
