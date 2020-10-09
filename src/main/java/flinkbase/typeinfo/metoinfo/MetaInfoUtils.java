package flinkbase.typeinfo.metoinfo;

import org.apache.flink.api.java.Utils;

/**
 * 工具类，用来生成自己的内部第4层信息，也就是
 *  Utils.getCallLocationName(); 返回的是哪个方法
 */
public class MetaInfoUtils {
    class Inner{
        void sayHello2(){
            String callLocationName = Utils.getCallLocationName(4);
            // sayHello(MetaInfoUtils.java:20)
            System.out.println(callLocationName);
        };
    }
    public void sayHello(){
        String callLocationName = Utils.getCallLocationName(4);
        // main(MetaInfoUtils.java:25)
        System.out.println(callLocationName);
        new Inner().sayHello2();
    }
    public static void main(String[] args) {
        String callLocationName = Utils.getCallLocationName(4);
        // <unknown>
        System.out.println(callLocationName);
        new MetaInfoUtils().sayHello();
    }
}
