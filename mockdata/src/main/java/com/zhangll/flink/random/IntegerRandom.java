package com.zhangll.flink.random;

import java.util.Random;

public class IntegerRandom implements RandomType{

    public static int random(){
        int i = new Random().nextInt(100);
        System.out.println(i);
        return i;
    }

    @Override
    public boolean isCurrentType(Class<?> type) {
        return type == Integer.class || type == int.class;
    }
}
