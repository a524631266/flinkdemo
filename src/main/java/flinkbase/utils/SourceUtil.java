package flinkbase.utils;

import com.zhangll.flink.AnnotationMockContext;
import flinkbase.model.Address;
import flinkbase.model.Person;
import flinkbase.model.SubPerson;
import flinkbase.typeinfo.example.protocol.ProtoColType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class SourceUtil {
    /**
     * 产生随机的对象的source
     * @param sourceClass
     * @param <T>
     * @return
     */
    public static <T> SourceFunction<T> createStreamSource(Class<T> sourceClass){
        return new SourceFunction<T>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<T> ctx) throws Exception {
                AnnotationMockContext annotationMockContext = new AnnotationMockContext();
                while (running){
                    Object mock = annotationMockContext.mock(sourceClass);
//                    System.out.println(person);
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect((T) mock);
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        };
    }

    /**
     * 产生随机的对象的source
     * @param sourceClass
     * @param waterMarkerTimeField  水印时间birthDay字段名称
     * @param <T>
     * @return
     */
    public static <T> SourceFunction<T> createStreamSourceWithWatherMark(Class<T> sourceClass,String waterMarkerTimeField ){
        return new RichSourceFunction<T>() {

            private boolean running = true;
            @Override
            public void run(SourceContext<T> ctx) throws Exception {
                AnnotationMockContext annotationMockContext = new AnnotationMockContext();
                Field field = sourceClass.getDeclaredField(waterMarkerTimeField);
                field.setAccessible(true);
                while (running){
                    Object mock = annotationMockContext.mock(sourceClass);
                    if(mock instanceof Person){
                        Date o = (Date)field.get(mock);
                        ctx.collectWithTimestamp((T) mock, o.getTime());
                    } else{
                        ctx.collect((T) mock);
                    }

                    TimeUnit.MILLISECONDS.sleep(50);

                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        };
    }


    /**
     * 产生随机的对象的source
     * @param sourceClass
     * @param <T>
     * @return
     */
    public static <T> SourceFunction<ProtoColType<T>> createStreamSourceWrapperProtocol(Class<T> sourceClass){
        return new SourceFunction<ProtoColType<T>>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<ProtoColType<T>> ctx) throws Exception {
                while (running){
                    Random random = new Random();
                    int i = random.nextInt(100);
                    Person person = new Person();
                    person.setAge(i);
                    person.setAddress(new Address(0, "asddd"));
//                    System.out.println(person);
                    ProtoColType<Person> personProtoColType = new ProtoColType<Person>();
                    personProtoColType.rawObject = person;
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect((ProtoColType<T>) personProtoColType);
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        };
    }
    public static <T> SourceFunction<T> createStreamRichSource(Class<T> sourceClass){
        return new RichSourceFunction<T>() {
            private boolean running = true;
//            private ExecutionConfig configuration;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//                this.configuration =(ExecutionConfig) parameters;
            }

            @Override
            public void run(SourceContext<T> ctx) throws Exception {
                while (running){
                    T mock2 = (T) Mockito.mock(sourceClass, Mockito.withSettings().serializable());
//                    System.out.println(mock2);
//                    Person person = new Person(1, new Address(1,"asd"));
//                    ctx.collect((T) person);
//                    configuration.setClass(mock2.getClass().getName(), mock2.getClass());
//                    env.getConfig().registerPojoType(mock2.getClass());
                    System.out.println(mock2.getClass().getName());
//                    mock2.getClass().
                    ctx.collect(mock2);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
            };
        }
    /**
     * 产生随机的对象的source
     * @param sourceClass
     * @param <T>
     * @return
     */
    public static <T> SourceFunction<T> createStreamSubTypeSource(Class<T> sourceClass){
        return new SourceFunction<T>() {
            private boolean running = true;
            @Override
            public void run(SourceContext<T> ctx) throws Exception {
                while (running){
                    SubPerson subPerson = new SubPerson(true);
                    subPerson.setAddress(new Address(0,"asdf"));
                    T element = (T) subPerson;
                    ctx.collect(element);
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        };
    }


    public static <T> SourceFunction<T> createStateSource(Class<T> sourceClass){
        return new RichSourceFunction<T>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//                getRuntimeContext().getState(
//                        new ValueStateDescriptor<T>("person");
//                );
            }

            private boolean running = true;
            @Override
            public void run(SourceContext<T> ctx) throws Exception {
                while (running){
                    SubPerson subPerson = new SubPerson(true);
                    subPerson.setAddress(new Address(0,"asdf"));
                    T element = (T) subPerson;
                    ctx.collect(element);
                }
            }
            @Override
            public void cancel() {
                running = false;
            }
        };
    }
}
