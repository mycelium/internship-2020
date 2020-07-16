package ru.spbstu.amcp.internship.ParallelDBTaskExecutionApp;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestMain {

    public static void main(String[] args) {
//        String ddl = "CREATE TABLE `author5` (\n" +
//                "  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,\n" +
//                "  `name` varchar(100) NOT NULL unique,\n" +
//                "  `id2` int(11) DEFAULT NULL,\n" +
//                "  PRIMARY KEY (`id`),\n" +
//                "  KEY `id23` (`id2`),\n" +
//                "  CONSTRAINT `author4_ibfk_15` FOREIGN KEY (`id2`) REFERENCES `drivers2` (`id`)\n" +
//                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        String ddl2 = "CREATE TABLE `car` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(30) NOT NULL DEFAULT 'BMW',\n" +
                "  `user_name` varchar(30) DEFAULT NULL,\n" +
                "  `user_id` int(11) DEFAULT NULL,\n" +
                "  `value` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autointf` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autoinccol` int(11) NOT NULL DEFAULT 6,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `unique_name` (`name`),\n" +
                "  UNIQUE KEY `car_idx` (`name`),\n" +
                "  KEY `distfk` (`user_id`,`user_name`),\n" +
                "  KEY `car_upper_idx` (`value`),\n" +
                "  CONSTRAINT `distfk` FOREIGN KEY (`user_id`, `user_name`) REFERENCES `public`.`users` (`id`, `name`) ON DELETE CASCADE ON UPDATE CASCADE,\n" +
                "  CONSTRAINT `car_check` CHECK (`value` > 50 and `autointf` < 100)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

//        Scanner sc = new Scanner(ddl);
//
//        while(sc.hasNextLine()) {
//            String line = sc.nextLine();
//            Pattern p = Pattern.compile("NOT NULL");
//            Matcher m = p.matcher(line);
//            if(m.find()){
//                System.out.println(line.replaceAll(".*`(.*)`.*", "$1"));
//                System.out.println(line);
//            }
//        }

        Scanner sc = new Scanner(ddl2);

//        while(sc.hasNextLine()) {
//            String line = sc.nextLine();
//            if(line.contains("NOT NULL")){
//                System.out.println("NOT NULL: " + line.replaceAll(".*`(.*)`.*", "$1"));
//                System.out.println(line);
//            }
//
//            if(line.contains("DEFAULT")){
//                System.out.println("DEFAULT: " + line.replaceAll(".*`(.*)`.*", "$1"));
//                System.out.println(line);
//            }
//
//            if(line.contains("PRIMARY KEY")){
//                System.out.println("PK: " + line.replaceAll(".*`(.*)`.*", "$1"));
//                System.out.println(line);
//            }
//
//            if(line.contains("KEY") && !line.contains("PRIMARY KEY")){
//                System.out.println("KEY: " + line.replaceAll(".*`(.*)`.*", "$1"));
//                System.out.println(line);
//            }

        String line = "  `val2` int(11) NOT NULL DEFAULT 7 CHECK ((`val1` - `val2`) * 2 > 100)";
        System.out.println(line
                .replaceAll("(.*)CHECK \\((.*)\\)", "$1"));

    }

}


