package flinkbase.source;

import flinkbase.model.Person;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.codehaus.jackson.map.SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS;
import static org.junit.Assert.*;

public class SocketSourceTest {
    private ObjectMapper objectMapper;

    @Before
    public void init(){
        objectMapper = new ObjectMapper();
//        objectMapper.setSerializationInclusion(JsonSerialize.Inclusion.ALWAYS);
//        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS , false);
    }

    /**
     * {"age": 12, "name": "zhangll", "address":{"id":12,"address":"乘火车"},"birthDay": "2020-10-12"}
     * {"age": 12, "name": "zhangll",birthDay: "2020-10-12"}
     * @throws IOException
     */
    @Test
    public void testMapper() throws IOException {
        String data = "{\"age\": 12, \"name\": \"zhangll\", \"address\":{\"id\":12,\"address\":\"乘火车\"},\"birthDay\": \"2020-09-12\"}";
//        objectMapper.configure(WRITE_DATES_AS_TIMESTAMPS, false);
//        DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
//        objectMapper.setDateFormat(fmt);
        System.out.println(data);
        Person person = objectMapper.readValue(data, Person.class);
        System.out.println(person);
    }
    @Data
//    @AllArgsConstructor
//    @NoArgsConstructor
    static class ID{
        private int id;
    }
    /**
     * {"id": 10}
     * @throws IOException
     */
    @Test
    public void testIdMapper() throws IOException {
//        objectMapper.configure(Des)
        String data = "{\"id\": 10}";
        System.out.println(data);
        ID id = objectMapper.readValue(data, ID.class);
        System.out.println(id);
    }
}