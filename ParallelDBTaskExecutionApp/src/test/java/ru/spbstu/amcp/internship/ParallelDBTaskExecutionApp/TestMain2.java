package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public class TestMain2 {

    static InheritableThreadLocal<Integer> a = new InheritableThreadLocal<Integer>();


    public static void main(String[] args) throws InterruptedException {

//        new Thread(()->{
////
////            a.set(5);
////            new Thread(()->{
////                System.out.println("" + a.get());
////            }).start();
////
////        }).start();
////
////        new Thread(()->{
////            System.out.println("" + a.get());
////        }).start();


    }


}
