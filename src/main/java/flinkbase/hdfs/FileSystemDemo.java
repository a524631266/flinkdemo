package flinkbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemDemo {
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
//        FileSystem.get("hdfs://", config);
        LocalFileSystem local = FileSystem.getLocal(config);
        FSDataOutputStream output = local.create(new Path("data/hdfs/a.txt"));
        output.write("hello worldfased".getBytes());
        output.flush();
        output.close();

    }
}
