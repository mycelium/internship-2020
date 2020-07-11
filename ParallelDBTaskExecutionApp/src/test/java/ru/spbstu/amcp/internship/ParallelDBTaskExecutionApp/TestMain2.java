package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class TestMain2 {

    static InheritableThreadLocal<Integer> a = new InheritableThreadLocal<Integer>();
    static Integer b = new Integer(5);

    static ExecutorService exec = Executors.newFixedThreadPool(5);

    public static void main(String[] args) throws InterruptedException {

        int abb = 100_000_000;

        Queue<Object> a = new ConcurrentLinkedDeque<>();
        if(a.poll() == null){
            System.out.println("Hello123123");
        }
//
        CompletableFuture.runAsync(()->{
            System.out.println("Hello");
        }, exec).thenRunAsync(()->{
            System.out.println("Hello2");
        },exec);

        exec.shutdown();
    }



}
