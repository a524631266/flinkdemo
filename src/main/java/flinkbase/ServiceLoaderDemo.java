package flinkbase;

import org.apache.flink.core.execution.PipelineExecutorFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

public class ServiceLoaderDemo {
    public static void main(String[] args) {
        final ServiceLoader<PipelineExecutorFactory> loader =
                ServiceLoader.load(PipelineExecutorFactory.class);
        Iterator<PipelineExecutorFactory> iterator = loader.iterator();
        while (iterator.hasNext()){
            PipelineExecutorFactory next = iterator.next();
            System.out.println(next.getName());
        }
    }
}
