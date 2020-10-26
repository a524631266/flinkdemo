package flinkbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileNormalLargeWriter {
    public static Configuration config = new Configuration();
    public static String uri = "hdfs://hadoop01:8020";

    public static void main(String[] args) throws IOException {
        // 获取操作系统
        LocalFileSystem fs = FileSystem.getLocal(config);
        String tmp = "data/tmp/abc.seq";
        Path path = new Path(tmp);

        // 设置参数
        SequenceFile.Writer.Option conf1 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
        SequenceFile.Writer.Option conf2 = SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option conf3 = SequenceFile.Writer.valueClass(Text.class);
        SequenceFile.Writer.Option conf4 = SequenceFile.Writer.file(path);

        // 设置writer
        SequenceFile.Writer writer = SequenceFile.createWriter(config, conf1, conf2, conf3, conf4);

        IntWritable key = new IntWritable();
        Text value = new Text();

        writer.append(key, value);

        IOUtils.closeStream(writer);
    }
}
