package fink02.stream.typeinfo.javacore;


import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

/**
 * 三种级别
 * @param <Y>
 */
public class GenericDeclarationDemo<Y> {
    public <U> GenericDeclarationDemo(U u){

    }
    public static void main(String[] args) {
        // 类级别的
        TypeVariable<Class<GenericDeclarationDemo>>[] typeParameters = GenericDeclarationDemo.class.getTypeParameters();
        for (TypeVariable<Class<GenericDeclarationDemo>> typeParameter : typeParameters) {

            Class<GenericDeclarationDemo> genericDeclaration = typeParameter.getGenericDeclaration();
        }
        Constructor<?>[] declaredConstructors = GenericDeclarationDemo.class.getDeclaredConstructors();
        for (Constructor<?> declaredConstructor : declaredConstructors) {
            Type[] genericParameterTypes = declaredConstructor.getGenericParameterTypes();
            for (Type genericParameterType : genericParameterTypes) {
                System.out.println(genericParameterType.getTypeName());
            }
        }
    }

}
