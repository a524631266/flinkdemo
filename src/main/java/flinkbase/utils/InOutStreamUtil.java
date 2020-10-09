package flinkbase.utils;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import scala.Tuple2;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class InOutStreamUtil {
    public static Tuple2<DataInputViewStreamWrapper, DataOutputViewStreamWrapper> generatePair()
    {
        PipedInputStream in = new PipedInputStream(1024);
        PipedOutputStream out = null;
        try {
            out = new PipedOutputStream(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DataInputViewStreamWrapper inWapper = new DataInputViewStreamWrapper(in);
        DataOutputViewStreamWrapper outWapper = new DataOutputViewStreamWrapper(out);

        return  new Tuple2<>(inWapper, outWapper);
    }
}
