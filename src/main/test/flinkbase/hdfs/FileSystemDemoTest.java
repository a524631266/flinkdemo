package flinkbase.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class FileSystemDemoTest {
    public static String TEST_PATH = "data/hdfs/b.txt";
    public Configuration configuration;
    public LocalFileSystem localFileSystem;
    @Before
    public void setUp() throws IOException {
        configuration = new Configuration();
        configuration.set("dfs.replication", String.valueOf(3));
        localFileSystem = FileSystem.getLocal(configuration);
    }


    @Test
    public void testCreateFile() throws IOException {
        String data = "asdfad";

        FSDataOutputStream output = localFileSystem.create(new Path(TEST_PATH));
        output.write(data.getBytes());
        output.flush();
        output.close();

    }
    @Test
    public void testReadMetaInfo() throws IOException {
        FileStatus file = localFileSystem.getFileLinkStatus(new Path(TEST_PATH));
        long accessTime = file.getAccessTime();
        long blockSize = file.getBlockSize();
        String group = file.getGroup();

        long len = file.getLen();
        short replication = file.getReplication();
        print(accessTime, blockSize, group, len, replication);
    }

    @Test
    public void testReadBlockMetaInfo() throws IOException {
        FileStatus file = localFileSystem.getFileLinkStatus(new Path(TEST_PATH));
        BlockLocation[] blocks = localFileSystem.getFileBlockLocations(file, 0, file.getLen());
        for (BlockLocation block : blocks) {
            String[] hosts = block.getHosts();
            for (String host : hosts) {
                System.out.println("host:" + host);
            }
            String[] names = block.getNames();
            for (String name : names) {
                System.out.println("name "+ name);
            }
            String[] topologyPaths = block.getTopologyPaths();
            for (String topologyPath : topologyPaths) {
                System.out.println("topologyPath:" + topologyPath);
            }
        }
    }


    private void print(long accessTime, long blockSize, String group, long len, long replication) {
        System.out.println("accessTime:" + accessTime);
        System.out.println("blockSize:" + blockSize);
        System.out.println("group:" + group);
        System.out.println("len:" + len);
        System.out.println("replication:" + replication);
    }


    @After
    public void setDown() throws IOException {
        localFileSystem.close();
    }
}