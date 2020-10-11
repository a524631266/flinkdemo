package flinkbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceFileNormalReader {
    public static Configuration config = new Configuration();
    public static String uri = "hdfs://hadoop01:8020";
    public static void main(String[] args) throws IOException {

        LocalFileSystem fileSystem = FileSystem.getLocal(config);
        Path path = new Path("data/hdfs/SequenceZip.seq");
        betterReaderTo(fileSystem, path);
    }

    public static void betterReaderTo(FileSystem fileSystem, Path path) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, config);


        Writable keyClass = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), config);
        Writable valueClass = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), config);

        while(reader.next(keyClass, valueClass)){
            System.out.println("key: " + keyClass);
            System.out.println("value: " + valueClass);
            System.out.println("position: " + reader.getPosition());
        }
        IOUtils.closeStream(reader);
    }

    public static void specialReader(LocalFileSystem fileSystem, Path path) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, config);
        // 其中一种读法
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)) {
            String s = key.toString();
            String s1 = value.toString();
            System.out.println("key: " + s + "; value: " + s1);
        }
        reader.close();
    }
}
