package flinkbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class CommandAndDeclarativeDemo {
    /**
     * 一步一步，新手，繁琐
     */
    public static void Command(){
        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        List<Integer> tempList = new ArrayList<>();
        for (Integer value : data) {
            tempList.add(value * 2);
        }
        int result = 0 ;
        for (Integer integer : tempList) {
            result += integer;
        }
        System.out.println(result);
    }

    /**
     * 声明式更简洁，只要一个任务，
     * 老手，不需要很详细，
     * select sum(1 * value) from data
     */
    public static void Declarative(){
        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        IntStream intStream = data.stream().mapToInt(v -> v * 2);
        int sum = intStream.sum();
        System.out.println(sum);
    }


    public static void main(String[] args) {
        Command();
        Declarative();
    }
}
