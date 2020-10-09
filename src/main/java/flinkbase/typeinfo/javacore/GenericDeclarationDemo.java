package flinkbase.typeinfo.javacore;


import java.lang.reflect.*;

/**
 * 三种级别,获取范型
 * @param <Y>
 */
public class GenericDeclarationDemo<Y> {
    public <U> GenericDeclarationDemo(U u){

    }
    public <T> void say(T a){
        System.out.println(a);
    }

    public static void main(String[] args) throws NoSuchMethodException {
        classDeclaration();
        constructorDeclaration();
        methodDeclaration();

    }

    /**
     * method: T
     * public void fink02.stream.typeinfo.javacore.GenericDeclarationDemo.say(java.lang.Object)
     * @throws NoSuchMethodException
     */
    private static void methodDeclaration() throws NoSuchMethodException {
        /**
         * 通过方法获取
         */
        Method say = GenericDeclarationDemo.class.getDeclaredMethod("say", Object.class);
        Type[] genericParameterTypes = say.getGenericParameterTypes();
        for (Type genericParameterType : genericParameterTypes) {
            if(genericParameterType instanceof TypeVariable){
                String typeName = genericParameterType.getTypeName();
                System.out.println("method: " + typeName);
                GenericDeclaration method = ((TypeVariable) genericParameterType).getGenericDeclaration();
                System.out.println(method);
            }
        }
    }

    /**
     * construct:  U
     * public fink02.stream.typeinfo.javacore.GenericDeclarationDemo(java.lang.Object)
     */
    private static void constructorDeclaration() {
        Constructor<?>[] declaredConstructors = GenericDeclarationDemo.class.getDeclaredConstructors();
        for (Constructor<?> declaredConstructor : declaredConstructors) {
            Type[] genericParameterTypes = declaredConstructor.getGenericParameterTypes();
            for (Type genericParameterType : genericParameterTypes) {
                if(genericParameterType instanceof TypeVariable){
                    String typeName = genericParameterType.getTypeName();
                    System.out.println("construct:  " + typeName);
                    GenericDeclaration constructor = ((TypeVariable) genericParameterType).getGenericDeclaration();
                    System.out.println(constructor);
                }
            }
        }
    }

    /**
     * class: fink02.stream.typeinfo.javacore.GenericDeclarationDemo
     */
    private static void classDeclaration() {
        // 类级别的
        TypeVariable<Class<GenericDeclarationDemo>>[] typeParameters = GenericDeclarationDemo.class.getTypeParameters();
        for (TypeVariable<Class<GenericDeclarationDemo>> typeParameter : typeParameters) {

            Class<GenericDeclarationDemo> classDeclaration = typeParameter.getGenericDeclaration();
            // class: fink02.stream.typeinfo.javacore.GenericDeclarationDemo
            System.out.println("class: " + classDeclaration.getName());
        }
    }

}
