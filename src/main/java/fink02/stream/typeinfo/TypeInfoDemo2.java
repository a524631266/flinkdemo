package fink02.stream.typeinfo;

import fink02.model.Person;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.mockito.Mockito;

import java.util.ArrayList;

public class TypeInfoDemo2 {
    public static void main(String[] args) {

        //  BasicTypeInfo
        TypeInformation<String> string = TypeInformation.of(String.class);
        System.out.println(string);
        // IntegerTypeInfo
        TypeInformation<Integer> int2 = TypeInformation.of(int.class);
        System.out.println(int2);

        // GenrericTypeInfo
        TypeInformation<ArrayList> arrayList = TypeInformation.of(ArrayList.class);
        System.out.println(arrayList);
        // PojoTypeinfo
        TypeInformation<Person> person = TypeInformation.of(Person.class);
        System.out.println(person);
        // GenericTypeInfo shit了
        TypeInformation<? extends Person> person_cglib = TypeInformation.of(Mockito.mock(Person.class).getClass());
        System.out.println(person_cglib);
        // PojoTypeinfo
        TypeInformation<? extends Person> newPerson = TypeInformation.of(new Person().getClass());
        System.out.println(newPerson);
    }
}
