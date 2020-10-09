package flinkbase.typeinfo;

import flinkbase.model.Person;
import flinkbase.typeinfo.model.PersonMessage;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.types.Either;
import org.mockito.Mockito;

import java.lang.reflect.Type;
import java.util.ArrayList;
/**
 * @see TypeExtractor#createTypeInfo(Type)
 *
 * ArrayList -> GenrericTypeInfo
 * String -> BasicTypeInfo
 *
 */
public class TypeInfoDemo2 {

    public static void main(String[] args) {
        System.out.println("###########stringType###########");
        stringType();
        System.out.println("###########intType###########");
        intType();
        System.out.println("###########pojoType###########");
        pojoType();
        System.out.println("###########generalType###########");
        generalType();
        System.out.println("###########enumType###########");
        enumType();
        System.out.println("###########eitherType###########");
        eitherType();
        System.out.println("###########optionType###########");
        optionType();
//        Double[] doubles = new Double[];
        TypeInformation<? extends Double[]> doubleArray = TypeInformation.of(new Double[]{}.getClass());
        System.out.println(doubleArray);
    }

    private static void optionType() {


    }

    private static void enumType() {
        TypeInformation<Gender> enum2 = TypeInformation.of(Gender.Femle.getDeclaringClass());
        System.out.println(enum2);
    }

    private static void generalType() {
        // GenrericTypeInfo
        TypeInformation<ArrayList> arrayList = TypeInformation.of(ArrayList.class);
        System.out.println(arrayList);
        // GenericTypeInfo shit了
        TypeInformation<? extends Person> person_cglib = TypeInformation.of(Mockito.mock(Person.class).getClass());
        System.out.println(person_cglib);
        // protobuf类型
        TypeInformation<PersonMessage.PersonModel> personMessage = TypeInformation.of(PersonMessage.PersonModel.class);
        System.out.println(personMessage);
        // 无参构造函数是私有的,即使有public类型的有参构造
//        TypeInformation<PersonModel> personModel = TypeInformation.of(PersonModel.class);
//        System.out.println(personModel);

    }

    private static void pojoType() {
        // PojoTypeinfo
        TypeInformation<Person> person = TypeInformation.of(Person.class);
        System.out.println(person);
        // PojoTypeinfo
        TypeInformation<? extends Person> newPerson = TypeInformation.of(new Person().getClass());
        System.out.println(newPerson);
    }

    private static void intType() {
        // IntegerTypeInfo
        TypeInformation<Integer> integer = TypeInformation.of(Integer.class);
        System.out.println(integer.getTypeClass());
        // IntegerTypeInfo
        TypeInformation<Integer> int2 = TypeInformation.of(int.class);
        System.out.println(int2);
    }

    private static void stringType() {
        /**
         * @see BasicTypeInfo
         */
        TypeInformation<String> string = TypeInformation.of(String.class);
        System.out.println(string);
    }

    enum Gender{
        Male,Femle;
    }

    /**
     * either的使用配合isLeft再提取left
     * 类似于迭代器模式
     *
     * Type extraction is not possible on Either type as it does not contain information about the 'left' type.
     */
    public static void eitherType(){
        Either<String, Object> either = Either.Left("123");
//        TypeInformation<? extends Either> eitherType = TypeInformation.of(either.getClass());
//        if(either.getClass() instanceof ParameterizedType){
//
//        }
        TypeInformation<? extends Either> eitherType = TypeExtractor.createTypeInfo(either.getClass());
        System.out.println(eitherType);
        boolean left = either.isLeft();
        String left1 = null;
        if (left) {
            left1 = either.left();
        }
        boolean right = either.isRight();
        Object right1 = null;
        if (right) {
            right1 = either.right();
        }
        System.out.println(String.format("left: %b, right: %b", left, right));
        System.out.println(String.format("left: %s, right: %s", left1, right1));
    }
}
