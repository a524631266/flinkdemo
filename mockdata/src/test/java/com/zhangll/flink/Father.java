package com.zhangll.flink;

import lombok.ToString;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@ToString
public class Father {
    private int age;
    private Integer id;
    private String name;
    private String Address;
    private double money_d;
    private float money_f;
    private char firstName;
    private Character a = 'c';
    // 男为1 女为0
    private boolean sex;
    private ArrayList<Integer> sonsNameList1;
    private List<Integer> sonsNameList2;
    private LinkedList<Integer> sonsNameList3;

    private Set<String> sonsName2;

}
