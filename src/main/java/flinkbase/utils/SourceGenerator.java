package flinkbase.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SourceGenerator {

    public static List generate3tupleList(){
        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<Tuple3<Integer, Integer, Integer>>();

        data.add(new Tuple3<>(0 , 1, 0));
        data.add(new Tuple3<>(0 , 1, 1));
        data.add(new Tuple3<>(0 , 2, 2));
        data.add(new Tuple3<>(0 , 1, 3));
        data.add(new Tuple3<>(1 , 2, 5));
        data.add(new Tuple3<>(1 , 2, 9));
        data.add(new Tuple3<>(1 , 2, 11));
        data.add(new Tuple3<>(1 , 2, 13));
        return data;
    }


    public static List generate3tupleList(int count){
        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<Tuple3<Integer, Integer, Integer>>();
        for (int i = 0; i < count; i++) {
            int first = new Random().nextInt(6);
            int s = new Random().nextInt(10);
            int t = new Random().nextInt(20);
            data.add(new Tuple3<>(first, s ,t ));
        };
        return data;
    }
}
