package fink02.stream.typeinfo.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
        MapFunction<Boolean, MyEither<Boolean, String>> mapFunction = new MyEitherMapper<Boolean, String>();
        TypeInformation<MyEither<Boolean, String>> mapReturnTypes = TypeExtractor.getMapReturnTypes(mapFunction, Types.BOOLEAN);
        assertTrue(mapReturnTypes instanceof EitherTypeInfo);
        // 脱去外皮

        EitherTypeInfo newTypeInfo = (EitherTypeInfo) mapReturnTypes;
        assertEquals(newTypeInfo.getLeftType() , Types.BOOLEAN);
        assertEquals(newTypeInfo.getRightType() , Types.STRING);

    }




    @Parameterized.Parameters
    public static Object[][] initParameters() {
        return new Object[][]{{0, 0}, {1, 1}, {2, 1}, {3, 2}, {4, 3},
                {5, 5}, {6, 8}};
    }
}