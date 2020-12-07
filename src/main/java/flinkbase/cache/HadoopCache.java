package flinkbase.cache;

import com.zhangll.jmock.core.annotation.BasicTokenInfo;
import flinkbase.utils.FocusUtil;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * hdfs 分布式缓存解决思路
 * @see https://www.cnblogs.com/wyh-study/p/12925769.html
 */
public class HadoopCache {
    @ToString
    @Data
    class Entity{
        //        @BasicTokenInfo("/0000000[1-9]/")
        @BasicTokenInfo("/[1-9]/")
        String entity;
        @BasicTokenInfo("00000000")
        String parentId;
    }
    public static void main(String[] args) {
        SingleOutputStreamOperator<Entity> sourceStream = FocusUtil.generateEnableSourceStream(
                Entity.class, 500L,
                "hdfs://192.168.10.61:8020/flink/cache/dimension.txt", "testCache");

        sourceStream.map(new RichMapFunction<Entity, String>() {
//            ArrayList<String> dataList = new ArrayList<>();
            Map<String, String> map = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File testCache = getRuntimeContext().getDistributedCache().getFile("testCache");
                List<String> strings = FileUtils.readLines(testCache);
                // 迭代读取流
                FileInputStream input = new FileInputStream(testCache);
                InputStreamReader reader = new InputStreamReader(input);
                BufferedReader bufferedReader = new BufferedReader(reader);
                String line = bufferedReader.readLine();
                while (line != null) {
                    String[] split = line.split("\t");
                    if(split.length>=2){
                        String key = split[0];
                        String value = split[1];
                        map.put(key, value);
                    }
                    line = bufferedReader.readLine();
                }
                // 关闭流
                IOUtils.closeQuietly(input);
//                dataList.addAll(strings);
//                System.out.println("分布式缓存:" + strings);
            }

            @Override
            public String map(Entity value) throws Exception {
                System.out.println("#######################");
                String result = map.get(value.entity + "");
//                System.out.println(result);
                System.out.println("#######################");
                return result;
            }
        }).print();
//        sourceStream.print();


        FocusUtil.execute("hadoop cache");
    }
}
