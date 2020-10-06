package fink02.stream.typeinfo.javacore;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;

/**
 * 其中，& 后必须为接口；
 * @param <T>
 */
public class JavaType<T extends String & Serializable & Comparable<String> > {
    public List<T> list;
    public Set<T> set;
    public Map<String, Integer> map;
    public List<T>[] lists;
    // 编译之后可以直接定位　为String类型的parameterized类型的数据
    public List<String>[] stringLists;
    public static void main(String[] args) throws NoSuchFieldException {

        parameterTest();
    }

    public static void parameterTest() throws NoSuchFieldException {
        listType();
        mapType();
        listsTypes();

        genericArrayType();

        System.out.println("----------------------------------");

        // generic declaration
        //



    }

    private static void genericArrayType() throws NoSuchFieldException {
        Type stringLists = JavaType.class.getDeclaredField("stringLists").getGenericType();
        if (stringLists instanceof GenericArrayType){
            // genericArrayType中可以通过获取元素类型方式
            String typeName = stringLists.getTypeName();
            // 因为是范型的所以是范型类,仍然是java.util.List，类型擦除
            Type genericComponentType = ((GenericArrayType) stringLists).getGenericComponentType();
            if(genericComponentType instanceof ParameterizedType){
                Type[] actualTypeArguments = ((ParameterizedType) genericComponentType).getActualTypeArguments();
                for (Type actualTypeArgument : actualTypeArguments) {
                    System.out.println("general :" + actualTypeArgument.getTypeName());
                }
            }
            System.out.println(typeName);
        }
    }

    private static void listsTypes() throws NoSuchFieldException {
        // genericArrayType : java.util.List<T>[]
        Type lists = JavaType.class.getDeclaredField("lists").getGenericType();
        if (lists instanceof GenericArrayType){
            // genericArrayType中可以通过获取元素类型方式
            String typeName = lists.getTypeName();
            // 因为是范型的所以是范型类
            Type genericComponentType = ((GenericArrayType) lists).getGenericComponentType();

            System.out.println(typeName);
        }
    }

    private static void mapType() throws NoSuchFieldException {
        // map
        Type mapType = JavaType.class.getDeclaredField("map").getGenericType();
        if ((mapType instanceof ParameterizedType)) {
            Type[] actualTypeArguments = ((ParameterizedType) mapType).getActualTypeArguments();
            for (Type actualTypeArgument : actualTypeArguments) {
                System.out.println("map actual Type: " + actualTypeArgument.getTypeName());
            }
        }
    }

    private static void listType() throws NoSuchFieldException {
        //java.util.List<T> 是一个ParameterizedType,带有范型参数的Type
//        Type type = stringJavaType.getClass().getDeclaredField("list").getGenericType();
        Type type = JavaType.class.getDeclaredField("list").getGenericType();
        // java.util.List<T>
        if ((type instanceof ParameterizedType)) {
            // 获取范型类的所有范型参数列表
            Type[] actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
            for (Type actualTypeArgument : actualTypeArguments) {
                System.out.println("actualType:" + actualTypeArgument.getTypeName());
                if(actualTypeArgument instanceof TypeVariable){
                    Type[] bounds = ((TypeVariable) actualTypeArgument).getBounds();
                    for (Type bound : bounds) {
                        System.out.println("bound type:" + bound.getTypeName());
                        if(bound instanceof ParameterizedType){
                            System.out.println("is Parameterized Type");
                            Type[] actualTypeArguments1 = ((ParameterizedType) bound).getActualTypeArguments();
                            for (Type type1 : actualTypeArguments1) {
                                System.out.println("inner ytpe:" + type1.getTypeName());
                            }
                        }
                    }
                    // 被范型约束的定义 即 T 定义的容器类
                    // generic Declaration:class fink02.stream.typeinfo.JavaType
                    GenericDeclaration genericDeclaration = ((TypeVariable) actualTypeArgument).getGenericDeclaration();
                    System.out.println("generic Declaration:" + genericDeclaration);
                }
            }
            // 获取范型类的,无参类
            Type rawType = ((ParameterizedType) type).getRawType();
            // java.util.List
            String typeName = rawType.getTypeName();
            System.out.println("raw type :" + typeName);
            System.out.println("parameterized type : " + type.getTypeName());
        }
    }
}
