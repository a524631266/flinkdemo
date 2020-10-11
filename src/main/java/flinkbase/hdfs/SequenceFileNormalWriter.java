package flinkbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class SequenceFileNormalWriter {
    public static Configuration config = new Configuration();
    public static String uri = "hdfs://hadoop01:8020";
    public static void main(String[] args) throws IOException {
//        URI uri = URI.create(SequenceFIleWriter.uri);
        LocalFileSystem fileSystem = FileSystem.getLocal(config);
//        FileSystem fileSystem = FileSystem.newInstance(uri, config);
        // 设置Key
        IntWritable key = new IntWritable();
        Text value = new Text();
//        FSDataOutputStream fsDataOutputStream = fileSystem.create();
        Path path = new Path("data/hdfs/Sequence.seq");

        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, config, path, IntWritable.class, Text.class);

        String[] data = {"a,b,c,d,e,f", "g,h,i,j,k", "l,m,n,o,p"};

        for (int i = 0; i < 10; i++) {
            key.set(i);
            value.set(data[i % data.length]);
            writer.append(key, value);
        }
//        writer.
//        writer.hflush();
        IOUtils.closeStream(writer);

        SequenceFileNormalReader.betterReaderTo(fileSystem, path);


    }
}
