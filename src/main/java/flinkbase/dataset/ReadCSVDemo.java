package flinkbase.dataset;

import flinkbase.utils.EnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;

public class ReadCSVDemo {
    public static void main(String[] args) {
        ExecutionEnvironment env = EnvUtil.getLoalWebDataSetEnv();
        env.readCsvFile("");
    }
}
