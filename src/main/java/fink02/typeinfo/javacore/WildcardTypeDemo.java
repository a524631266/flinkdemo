package fink02.typeinfo.javacore;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.List;
import java.util.Map;

/**
 * wildCard 是作用与已经被范型圈定的类使用的,不能作为范型使用
 */
public class WildcardTypeDemo {
    public List<? extends String> list;
    public Map<? super String, ? extends Number> map;

    public static void main(String[] args) throws NoSuchFieldException {
        getUpper("list");
        getUpper("map");
    }

    /**
     * #############
     * start solve: list:java.util.List<? extends java.lang.String>
     * lowerBounds num: 0; upperBounds num: 1
     * upper Class: class java.lang.String
     * #############
     * start solve: map:java.util.Map<? super java.lang.String, ? extends java.lang.Number>
     * lowerBounds num: 1; upperBounds num: 1
     * upper Class: class java.lang.Object
     * lower Class:class java.lang.String
     * lowerBounds num: 0; upperBounds num: 1
     * upper Class: class java.lang.Number
     *
     * class java.lang.String
     * @throws NoSuchFieldException
     */
    private static void getUpper(String fieldName) throws NoSuchFieldException {
        // extends 可以确定上界
        Field list = WildcardTypeDemo.class.getField(fieldName);
        Type genericType = list.getGenericType();
        System.out.println("#############\nstart solve: " + fieldName + ":" + genericType.getTypeName() );
        if(genericType instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) genericType).getActualTypeArguments();
            for (Type actualTypeArgument : actualTypeArguments) {
                if (actualTypeArgument instanceof WildcardType) {
                    Type[] lowerBounds = ((WildcardType) actualTypeArgument).getLowerBounds();
                    Type[] upperBounds = ((WildcardType) actualTypeArgument).getUpperBounds();
                    System.out.println(String.format("lowerBounds num: %d; upperBounds num: %d", lowerBounds.length, upperBounds.length));
                    int upperSize = upperBounds.length;
                    for (int i = 0; i < upperSize; i++) {
                        Type upperBound = upperBounds[i];
                        System.out.println("upper Class: " + upperBound);
                    }

                    int lowerSize = lowerBounds.length;
                    for (int i = 0; i < lowerSize; i++) {
                        Type lowerBound = lowerBounds[i];
                        System.out.println("lower Class:" + lowerBound);
                    }


                }
            }
        }
    }
}
