# internship-2020


## Description

* Multi module Spring Boot project written during summer internship.

* The project contains a library which allows you to drop or restore constraints for several RDBMS
(MariaDB, SQLite, Postgres) and manage a transaction across multiple threads in Spring application on a service layer.


### Main features of library

1. Manage a transaction across multiple threads in Spring application; Form and run a chain of concurrent and sequential tasks that are performed in a single transaction 
    The reasons for implementing are as follows:
    * The Spring API works very well with almost all of the transaction management requirements as long as the transaction is on a single thread. 
The problem arises when we want to manage a transaction across multiple threads. Spring doesn't support transactions over multiple threads out of the box. 
Spring doesn't explicitly mention that in the documentation, but you will end up with runtime errors or unexpected results if you try to do so.
    * If we start the transaction from one thread and try to commit
or rollback the transaction from another thread, a runtime error will be generated complaining that the Spring transaction is not active on the current thread. Though we start and end the transaction from the same thread, we cannot perform database operations belong to transaction from another thread too.
    
    (see [Spring Transaction Management Over Multiple Threads](https://dzone.com/articles/spring-transaction-management-over-multiple-thread-1) and [Transaction Management](https://docs.spring.io/spring/docs/5.2.x/spring-framework-reference/data-access.html#transaction)
for more details)

2. Drop or restore one or several SQL table constraints for the following RDBS: MariaDB and
Postgres (now supports unique, PK, FK, default, not null, index, check constraints), SQLite ( you can only drop/restore indexes ). The reason for implementing:
    * sometimes to perform a large number of operations on a table (for example, a large number of inserts) it is necessary to drop constraint and / or delete and recreate the index. 

### List of all submodules

- concurdb - library in which the listed features are implemented.
- concurdbapp - Spring Boot application that demonstrates how you can use the library and implemented features. 

## Built With

- Maven - build automation tool used primarily for Java projects. 

## Getting Started

 #### Requirements
  - Linux or macOS or Windows
  - Java 14
  - One of the following RDBMSs is required: SQLite, MariaDB, PostgreSQL
  - JDBC driver (can be added as the dependency in pom.xml file) for chosen RDBMS

 #### Building library:
  - run `mvn package` in shell inside the concurdb subfolder to compile library and package it in .jar file.  
  - put library .jar file (for example `concurdb-0.0.1-SNAPSHOT.jar`) on your classpath 
  - add the missing dependencies to your pom.xml file. All the necessary dependencies and plugins for using concurdb are listed in concurdbapp pom.xml file.

## Examples

- Forming and running a chain of concurrent and sequential tasks that are performed in a single transaction
    
```js
        TransactionTemplate transactionTemplate;
        public void myTx(){
                //You need default Spring TransactionTemplate instance to run transaction
                ConcurrentTransactionManager ctxm = 
                new ConcurrentTransactionManager(transactionTemplate);    
                   
                //Make settings before starting the transaction.
                ctxm.setTxpolicy(
                TransactionRollbackPolicy
                .ROLLBACK_WHOLE_TX_ON_EXECUTION_EXCEPTION_IN_ANY_THREAD);
                ctxm.getTransactionTemplate()
                .setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
                ctxm.getTransactionTemplate()
                .setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                
                //Run the transaction
                ctxm.executeConcurrentTransaction(()->{
                    //Make new action in new thread
                    new TransactionAction(ctxm).startAction(()->{
                        dao.insert(new User(5, "Begin of innerAction"));
                        return null;
                    })
                   //Put new task in the same thread
                    .putAnotherAction(prev->{
                        dao.insert(new User(6, "Next action in inner"));
                        dao.insert(new User(7, "Next action in inner"));
                        return null;
                    })
                     //Put new task in other thread
                    .putAnotherActionAsync(prev->{
                        //Rollback example
                        Object savePoint1 = ctxm.createSavepoint();
                        TransactionAction inneraction = new TransactionAction(ctxm)
                        .startAction(()->{
                            dao.insert(new User(66,"Inner in Inner"));
                            return null;
                        }).putAnotherActionAsync(prevRes->{
                            dao.insert(new User(77, "Async Inner"));
                            return null;
                        });
                        try {
                            //block the thread and wait for the task to complete.
                            inneraction.get();
                        } catch (ExecutionException exception) {
                            exception.printStackTrace();
                        }
                        ctxm.rollbackToSavepoint(savePoint1);
                        return null;
                    });
        
                    return null;
                });
            }
```

- Dropping/restoring constraints in PostgreSQL

    - DDL:
    ```sql
        create table users
        (
        	id integer not null,
        	name varchar(255) not null,
        	constraint postgrespk
        		primary key (id, name)
        );
        create table test_schema.car
        (
        	id integer not null
        		constraint car_pkey
        			primary key,
        	name varchar(30)
        		constraint unique_name
        			unique,
        	user_name varchar(30),
        	user_id integer,
        	constraint distfk
        		foreign key (user_id, user_name) references users
        			on update cascade on delete cascade,
        );
    ```
   - Dropping FK:
    ```java
    public void foo(){
        //Init all constraints
        List<Constraint> cons = pcm.getAndInitAllConstraints("test_schema", "car");
        //Drop FK
        pcm.dropOneConstraint("test_schema", "car", "distfk", ConstraintType.FK);
        //Insert some data without FK
        for (int i = 0; i < 100; i++){
                    jdbcTemplate.execute("insert into test_schema.car (id, name, user_name, user_id)" +
                            " VALUES ("+i+", 'Test', 'name', 1)");
        }
        //Restore FK (Exception)
        pcm.restoreOneConstraint("test_schema", "car", "distfk", ConstraintType.FK,true);
    }
    ```
  - Dropping all constraints
   
   ```java
    public void foo(){
            List<Constraint> cons = pcm.getAndInitAllConstraints("test_schema", "car");

             pcm.dropAllConstraintsInTable("test_schema", "car", true,
                              ConstraintType.CHECK, ConstraintType.DEFAULT, 
                              ConstraintType.FK, 
                              ConstraintType.PK,
                              ConstraintType.INDEX, 
                              ConstraintType.NOT_NULL, ConstraintType.UNIQUE);
   
             pcm.restoreAllConstraintsInTable("test_schema", "car", true);
        }
  ```

## Running the tests

 - In module concurdb you can find several tests. To run them properly in each test class you need to configure the function that returns DataSource instance:
```java
    //For example in this case you should have Postgres installed and running on 127.0.0.1:5432
    //and also you need to have set DB: TestDB, username: "postgres" and password: "root".
    DataSource dataSource(){
            DriverManagerDataSource ds = new DriverManagerDataSource();
            ds.setDriverClassName("org.postgresql.Driver");
            ds.setUrl("jdbc:postgresql://127.0.0.1:5432/TestDB");
            ds.setUsername("postgres");
            ds.setPassword("root");
            return ds;
        }
```