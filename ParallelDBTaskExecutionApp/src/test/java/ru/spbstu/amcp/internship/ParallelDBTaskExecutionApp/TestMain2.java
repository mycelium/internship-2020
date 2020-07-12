package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class TestMain2 {

    static InheritableThreadLocal<Integer> a = new InheritableThreadLocal<Integer>();
    static Integer b = new Integer(5);

    static ExecutorService exec = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws InterruptedException, ExecutionException {


        Queue<Object> a = new ConcurrentLinkedDeque<>();
        if(a.poll() == null){
            System.out.println("123");
        }
//


//        CompletableFuture.runAsync(() -> {
//            System.out.println(Thread.currentThread().getName());
//        }, exec).thenRunAsync(() -> {
//            System.out.println(Thread.currentThread().getName());
//        }, exec).thenRunAsync(() -> {
//            System.out.println(Thread.currentThread().getName());
//        }, exec);
//
//        CompletableFuture<Integer> com = CompletableFuture.supplyAsync(()->{
//            try {
//                Thread.sleep(1000);
//                System.out.println("Name:" + Thread.currentThread().getName());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            return 5;
//        }, exec).thenApplyAsync(va->{
//            try {
//                Thread.sleep(2000);
//                System.out.println("Name:" + Thread.currentThread().getName());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            return 6;
//        },exec);
//
//        System.out.println(com.get());

        exec.shutdown();
    }



}
