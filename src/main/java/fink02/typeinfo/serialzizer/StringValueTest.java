package fink02.typeinfo.serialzizer;

import org.apache.flink.core.memory.*;
import org.apache.flink.types.JavaToValueConverter;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * @see DataInputView 视图层（内存中读取）
 *
 *
 * StringValue为一个String类，如何演示StringValue的性能
 *
 */
public class StringValueTest {
    public static void main(String[] args) throws IOException {
        useStringValue();
        // testSerializer
        testSerializer();
        // testPerformance
        String randomString = StringUtils.getRandomString(new Random(), 10000, 1024 * 1024 * 2);
        System.out.println(randomString.length());
//        System.out.println(randomString);
    }

    private static void testSerializer() throws IOException {
        System.out.println("start serialize ------------");
        //        序列化工作是否可靠
        String[] values = new String[]{
                "asdf", "", "asdfd"
        };
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(1000);
        DataOutputView out = new DataOutputViewStreamWrapper(outStream);
        for (String value : values) {
            StringValue stringValue = new StringValue(value);
            stringValue.write(out);
        }

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
        DataInputViewStreamWrapper streamWrapper = new DataInputViewStreamWrapper(inputStream);
        int num = 0;
        while (inputStream.available() > 0) {
            StringValue value = new StringValue();
            value.read(streamWrapper);
            System.out.println("inner read:" + value + "::" + (value.getValue().equals(values[num])));
            num++;
        }
        System.out.println("end serializer");
    }

    /**
     * 如何使用StringValue以及如何使用JavaToValueConverter 转换
     * @throws IOException
     */
    private static void useStringValue() throws IOException {
        StringValue stringValue = new StringValue();
        stringValue.append('a').append('b').append('e');

        StringValue toString = new StringValue("asdddd");

        boolean equals = toString.equals(stringValue);
        System.out.println("ab==asdddd :" + equals);

        toString.copyTo(stringValue);

        System.out.println("asdddd copy to stringvalue:"+stringValue);

        Value value = JavaToValueConverter.convertBoxedJavaType("123");

        if (value instanceof StringValue){
            System.out.println("value: " + ((StringValue) value).getValue());
        }

        Object o = JavaToValueConverter.convertValueType(new StringValue("123"));
        if(o.getClass() == String.class){
            System.out.println("String class: "+ o);
        }
    }


    static void testInOutView(){
//
////        stringValue.read(view);
//        DataOutputSerializer out = new DataOutputSerializer(10);
//        stringValue.write(out);
//        // in 无法直接读取
//        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
//
//        StringValue receive = new StringValue();
//        receive.read(in);
//
//
//
//        DataInputDeserializer in2 = new DataInputDeserializer("add".getBytes());
//        receive.read(in2);
//        String s1 = receive.toString();
//
//        System.out.println(s1);
    }
}
