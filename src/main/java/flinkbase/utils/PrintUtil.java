package flinkbase.utils;

import java.lang.reflect.Field;

public class PrintUtil {
    public static void printObjectFields(Object object){
        Field[] declaredFields = object.getClass().getDeclaredFields();
        try {
            for (Field declaredField : declaredFields) {
                declaredField.setAccessible(true);
                Object o = declaredField.get(object);
                System.out.println("{  " + declaredField.getName() + ":" + o + " }");
            }
        }catch ( Exception e ){

        }
    }
}
