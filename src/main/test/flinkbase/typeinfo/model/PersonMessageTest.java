package flinkbase.typeinfo.model;

import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class PersonMessageTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() throws InvalidProtocolBufferException {
        PersonMessage.PersonModel person = PersonMessage.PersonModel.newBuilder()
                .setId(0)
                .setEmil("5246312@qq.com")
                .setGender(PersonMessage.Gender.male)
                .addAddresses(PersonMessage.PersonModel.PersonAddress.newBuilder())
                .setName("zll")
                .build();

        System.out.println(person);
        // 序列化操作
        byte[] bytes = person.toByteArray();
        //
        PersonMessage.PersonModel person2 = PersonMessage.PersonModel.parseFrom(bytes);

        assertThat(person, equalTo(person2));
    }

    @Test
    public void testOuter(){
        OuterMessage.Outer.Builder builder = OuterMessage.Outer.newBuilder().setId(1)
                .addFoo(1)
                .addFoo(3);

//                .setInner(0, OuterMessage.Outer.Inner.newBuilder());
        OuterMessage.Outer build = builder.build();
        System.out.println(build);
    }
}