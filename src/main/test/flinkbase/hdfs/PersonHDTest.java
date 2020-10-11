package flinkbase.hdfs;


import flinkbase.model.Person;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.*;

public class PersonHDTest {

    private PersonHD person1;
    private PersonHD person2;
    private PersonHD person3;
    private PersonHD person4;
    private PersonHD person5;
    private PersonHD nullPerson1;
    private PersonHD nullPerson2;

    @Before
    public void init(){
        person1 = new PersonHD("zhangll", 20, "male");
        person2 = new PersonHD("zhangll", 20, "male");
        person3 = new PersonHD("zll", 20, "male");
        person4 = new PersonHD("zhangll", 19, "male");
        person5 = new PersonHD("zhangll", 20, "female");

        nullPerson1 = new PersonHD("zhangll", 20, "male");
        nullPerson2 = new PersonHD("zhangll", 20, "male");
        nullPerson1.setNull();
        nullPerson1.setNull();
    }


    @Test
    public void testPersonEqual(){


        assertTrue(person1.equals(person2));

        assertFalse(person1.equals(person3));
        assertFalse(person1.equals(person4));
        assertFalse(person1.equals(person5));

        assertFalse(person1.equals(null));
        Person person = new Person();
        assertFalse(person1.equals(person));


        assertFalse(nullPerson1.equals(person1));
        assertFalse(nullPerson1.equals(person3));
        assertFalse(nullPerson1.equals(person4));
        assertFalse(nullPerson1.equals(person5));

        assertFalse(nullPerson1.equals(nullPerson2));


    }
    @Test
    public void testHash(){
        int i = person3.hashCode();
        int i1 = person4.hashCode();
        int i2 = person5.hashCode();
        int i3 = nullPerson1.hashCode();
        System.out.println(String.format("%d, %d, %d, %d", i, i1, i2, i3));

    }

}