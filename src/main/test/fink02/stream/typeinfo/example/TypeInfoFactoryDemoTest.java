package fink02.stream.typeinfo.example;

import fink02.model.Person;
import fink02.stream.typeinfo.example.protocol.ProtoColType;
import fink02.stream.typeinfo.example.protocol.ProtoColTypeInfo;
import fink02.stream.typeinfo.example.protocol.ProtocolMapMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Type;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TypeInfoFactoryDemoTest {
    public final int a;
    public final int b;
    public TypeInfoFactoryDemoTest(int a, int b) {
        this.a = a;
        this.b = b;
    }

    /**
     * 创建一个类型，通过一个类/对象来获取类型
     */
    @Test
    public void testSimpleType() {
        TypeInformation<IntLike> typeInfo = TypeExtractor.createTypeInfo(IntLike.class);
        assertEquals(typeInfo, Types.INT);
        TypeInformation<IntLike> forClass = TypeExtractor.getForClass(IntLike.class);
        assertEquals(forClass, Types.INT);
        TypeInformation<IntLike> forObject = TypeExtractor.getForObject(new IntLike());
        assertEquals(forObject, Types.INT);
        System.out.println(String.format("a: %d, b: %d", a, b));
    }

    @Test
    public void testMyEitherType(){
//        TypeInformation<MyEither> typeInfo = TypeExtractor.createTypeInfo(MyEither.class);
//        assertEquals(typeInfo, TypeInformation.of(MyEither.class));

//        MyEither<String, IntLike> stringIntLikeMyEither = new MyEither<String, IntLike>();
//        TypeInformation<MyEither<String, IntLike>> forObject = TypeExtractor.getForObject(stringIntLikeMyEither);
//        assertEquals(forObject, Types.INT);
        MapFunction<Boolean, MyEither<Boolean, String>> mapFunction = new MyEitherMapper<Boolean>();
//        MapFunction<Boolean, MyEither<Boolean, String>> mapFunction = new MyEitherMapper<Boolean, String>();
//        MyEitherMapper<Boolean, String> mapper = new MyEitherMapper<>();
//        TypeInformation<MyEither<Boolean, String>> mapReturnTypes1 = TypeExtractor.getMapReturnTypes(mapper, Types.BOOLEAN);

        // 由于map function获取的是单个input TypeInfo，因此，在returnTypes的时候传递一个typeinfo
        TypeInformation<MyEither<Boolean, String>> mapReturnTypes = TypeExtractor.getMapReturnTypes(mapFunction, Types.BOOLEAN);
        assertTrue(mapReturnTypes instanceof EitherTypeInfo);
        // 脱去外皮
        EitherTypeInfo newTypeInfo = (EitherTypeInfo) mapReturnTypes;
        assertEquals(newTypeInfo.getLeftType() , Types.BOOLEAN);
        assertEquals(newTypeInfo.getRightType() , Types.STRING);

    }

    @Test
    public void testTypeExtractorUtilsStaticMethod(){
        // 剔除掉泛型的class
        Type superclass = TypeExtractionUtils.typeToClass(MyEither.class).getGenericSuperclass();
        assertTrue(superclass == null);
        // 判断是否是class
        boolean classType = TypeExtractionUtils.isClassType(MyEither.class);
    }

    /**
     * 假设有一个数据类型，
     */
    @Test
    public void testProtocolTypeInfo(){
        ProtocolMapMapper<Tuple3<Long, String, Integer>> mapper = new ProtocolMapMapper<>();
        Class<ProtoColType<Tuple3<Long, String, Integer>>> cls = null;
        TypeInformation<?> t = new TupleTypeInfo<Tuple3<Long, String, Integer>>(Types.LONG, Types.STRING, Types.INT);
        TypeInformation<ProtoColType<Tuple3<Long, String, Integer>>> inputType = new ProtoColTypeInfo(cls, t);
        TypeInformation<String> mapReturnTypes = TypeExtractor.getMapReturnTypes(mapper, inputType);
        assertTrue(mapReturnTypes instanceof BasicTypeInfo);
        ProtoColType<Person> personProtoColType = new ProtoColType<Person>();
        TypeInformation<ProtoColType> forClass = TypeExtractor.getForObject(personProtoColType);

        // 获取应哟个类型
        TypeInformation<ProtocolMapMapper<Tuple3<Long, String, Integer>>> forObject = TypeExtractor.getForObject(mapper);
        assertTrue(!forObject.isBasicType());
        assertTrue(!forObject.isTupleType());
        assertTrue(!forObject.isSortKeyType());
        assertTrue(!forObject.isKeyType());
    }



    @Parameterized.Parameters
    public static Object[][] initParameters() {
        return new Object[][]{{0, 0}, {1, 1}, {2, 1}, {3, 2}, {4, 3},
                {5, 5}, {6, 8}};
    }
}