package flinkbase.configuse;

import flinkbase.model.Person;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

/**
 * 使用建造者模式来创建不变ConfigOption对象（key：value）
 * 主要用来构建一个不可变的对象。名字叫做配置选项ConfigOption
 */
public class ConfigDemo {
    public static void main(String[] args) {
        // 利用ConfigOptions的key 创建一个builder，用来支持创建
        // 1. intType (基本类型，boolean，float，double,Long)
        // 2. stringType 字符串
        // 3. mapType map<String, String>类型
        // 4. enumType 枚举类型
        // 5. durationType
        // 6. memoryType
        ConfigOption<Integer> intV =
                ConfigOptions.key("asdbc").intType().defaultValue(1);
        System.out.println("intV desc:" + intV.description());
        System.out.println("intV defaultValue:" + intV.defaultValue());
        System.out.println("intV key:" + intV.key());

        ConfigOption<Type2> enumType = ConfigOptions.key("asdbc").enumType(Type2.class).defaultValue(Type2.A);
        Type2 type2 = enumType.defaultValue();
        System.out.println("enum: "+type2);

        // 配置文件相等性 只要key value 以及 内置的fallbackKeys相等就表示相等
        ConfigOption<Integer> intV2 = ConfigOptions.key("asdbc").intType().defaultValue(1);
        intV2.fallbackKeys().forEach(a -> {
            System.out.println("fallbackKeys:" + a.getKey());
        });
        System.out.println( intV.equals(intV2));
        assert intV.equals(intV2);
    }
    enum Type2 {
        A,B;
    }
}
