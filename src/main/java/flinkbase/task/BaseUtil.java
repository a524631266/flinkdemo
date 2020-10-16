package flinkbase.task;

import org.apache.flink.runtime.util.Hardware;

/**
 *
 */
public class BaseUtil {
    public static void main(String[] args) {
        int numberCPUCores = Hardware.getNumberCPUCores();
        System.out.println(numberCPUCores);

        long sizeOfPhysicalMemory = Hardware.getSizeOfPhysicalMemory();
        System.out.println(sizeOfPhysicalMemory);
    }
}
