package flinkbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;

public class SequenceFIleZipWriter {
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.getLocal(config);
        Path path = new Path("data/hdfs/SequenceZip.seq");
//        SequenceFile.Writer writer = SequenceFile.createWriter(fs, config, path, IntWritable.class, Text.class,
//                SequenceFile.CompressionType.RECORD, new BZip2Codec()
//        );
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, config, path, IntWritable.class, Text.class,
                SequenceFile.CompressionType.BLOCK, new BZip2Codec()
        );

        String[] data = {"a,b,c,d,e,f", "g,h,i,j,k", "l,m,n,o,p"};
        IntWritable key = new IntWritable();
        Text value = new Text();
        for (int i = 0; i < 10; i++) {
            key.set(i);
            value.set(data[i % data.length]);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);

        SequenceFileNormalReader.betterReaderTo(fs, path);
    }
}
