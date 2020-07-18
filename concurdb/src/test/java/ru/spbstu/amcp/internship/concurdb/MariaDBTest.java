package ru.spbstu.amcp.internship.concurdb;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.Constraint;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.ConstraintType;
import ru.spbstu.amcp.internship.concurdb.constraintsmanagement.MariaDBConstraintsManager;

import javax.sql.DataSource;
import java.util.List;

@SpringBootTest
public class MariaDBTest {


    MariaDBConstraintsManager mcm;

    JdbcTemplate jdbcTemplate;

    @Before
    public void init(){
        jdbcTemplate = new JdbcTemplate(dataSource());
        mcm = new MariaDBConstraintsManager(jdbcTemplate);
        Resource resource = new ClassPathResource("mariadbcar.sql");
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator(resource);
        databasePopulator.execute(jdbcTemplate.getDataSource());
    }


    @Test
    public void testMariaDBConstraintsManagerOnComplexFKAndAutoIncrement(){

        String ddlBeforeDropping = "CREATE TABLE `product_order` (\n" +
                "  `no` int(11) NOT NULL,\n" +
                "  `product_category` int(11) DEFAULT NULL,\n" +
                "  `product_id` int(11) DEFAULT NULL,\n" +
                "  `customer_id` int(11) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`no`),\n" +
                "  KEY `product_category` (`product_category`,`product_id`) USING BTREE,\n" +
                "  KEY `customer_id` (`customer_id`) USING BTREE,\n" +
                "  CONSTRAINT `product_order_ibfk_1` FOREIGN KEY (`product_category`, `product_id`) REFERENCES `product` (`category`, `id`) ON UPDATE CASCADE,\n" +
                "  CONSTRAINT `product_order_ibfk_2` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        String ddlAfterDropping = "CREATE TABLE `product_order` (\n" +
                "  `no` int(11) DEFAULT NULL,\n" +
                "  `product_category` int(11) DEFAULT NULL,\n" +
                "  `product_id` int(11) DEFAULT NULL,\n" +
                "  `customer_id` int(11) DEFAULT NULL\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        MariaDBConstraintsManager.REMOVE_AUTO_INCREMENT_BEFORE_PK = true;

        mcm.getAndInitAllConstraints("public","product_order");

        mcm.dropAllConstraintsInTable("public","product_order", true,
                ConstraintType.CHECK, ConstraintType.DEFAULT, ConstraintType.FK, ConstraintType.PK,
                ConstraintType.INDEX, ConstraintType.NOT_NULL, ConstraintType.UNIQUE);


        String tableDDL = jdbcTemplate.queryForObject("SHOW CREATE TABLE public.product_order;",
                (rs,i)->rs.getString(2));


        Assert.assertTrue(ddlAfterDropping.equals(tableDDL));

        mcm.restoreAllConstraintsInTable("public","product_order", true);


        tableDDL = jdbcTemplate.queryForObject("SHOW CREATE TABLE public.product_order;",
                (rs,i)->rs.getString(2));

        Assert.assertTrue(ddlBeforeDropping.equals(tableDDL));


        MariaDBConstraintsManager.REMOVE_AUTO_INCREMENT_BEFORE_PK = false;
    }


    @Test
    public void testMariaDBConstraintsManagerOnDroppingOneConstraint(){

        String ddlBeforeDropping = "CREATE TABLE `car` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(30) NOT NULL DEFAULT 'BMW',\n" +
                "  `user_name` varchar(30) DEFAULT NULL,\n" +
                "  `user_id` int(11) DEFAULT NULL,\n" +
                "  `value` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autointf` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autoinccol` int(11) NOT NULL DEFAULT 6,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `unique_name` (`name`,`value`),\n" +
                "  UNIQUE KEY `car_idx` (`name`),\n" +
                "  KEY `distfk` (`user_id`,`user_name`),\n" +
                "  KEY `car_upper_idx` (`value`),\n" +
                "  CONSTRAINT `distfk` FOREIGN KEY (`user_id`, `user_name`) REFERENCES `public`.`users` (`id`, `name`) ON DELETE CASCADE ON UPDATE CASCADE,\n" +
                "  CONSTRAINT `car_check` CHECK (`value` > 50 and `autointf` < 100)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        String ddlAfterDropping = "CREATE TABLE `car` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(30) NOT NULL,\n" +
                "  `user_name` varchar(30) DEFAULT NULL,\n" +
                "  `user_id` int(11),\n" +
                "  `value` int(11) NOT NULL,\n" +
                "  `autointf` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autoinccol` int(11) DEFAULT 6,\n" +
                "  KEY `distfk` (`user_id`,`user_name`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        List<Constraint> constraints = mcm.getAndInitAllConstraints("test_schema", "car");

        String tableDDL = jdbcTemplate.queryForObject("SHOW CREATE TABLE test_schema.car;",
                (rs,i)->rs.getString(2));


        Assert.assertTrue(tableDDL.equals(ddlBeforeDropping));

        mcm.dropOneConstraint("test_schema", "car", "autoinccol", ConstraintType.NOT_NULL);
        mcm.dropOneConstraint("test_schema", "car", "primary", ConstraintType.PK);
        mcm.dropOneConstraint("test_schema", "car", "distfk", ConstraintType.FK);
        mcm.dropOneConstraint("test_schema", "car", "name", ConstraintType.DEFAULT);
        mcm.dropOneConstraint("test_schema", "car", "user_id", ConstraintType.DEFAULT);
        mcm.dropOneConstraint("test_schema", "car", "value", ConstraintType.DEFAULT);
        mcm.dropOneConstraint("test_schema", "car", "car_check", ConstraintType.CHECK);
        mcm.dropOneConstraint("test_schema", "car", "unique_name", ConstraintType.UNIQUE);
        mcm.dropOneConstraint("test_schema", "car", "car_idx", ConstraintType.UNIQUE);
        mcm.dropOneConstraint("test_schema", "car", "car_upper_idx", ConstraintType.INDEX);

        tableDDL = jdbcTemplate.queryForObject("SHOW CREATE TABLE test_schema.car;",
                (rs,i)->rs.getString(2));

        Assert.assertTrue(tableDDL.equals(ddlAfterDropping));


        mcm.restoreOneConstraint("test_schema", "car", "car_upper_idx", ConstraintType.INDEX, true);
        mcm.restoreOneConstraint("test_schema", "car", "car_idx", ConstraintType.UNIQUE, true);
        mcm.restoreOneConstraint("test_schema", "car", "unique_name", ConstraintType.UNIQUE, true);
        mcm.restoreOneConstraint("test_schema", "car", "car_check", ConstraintType.CHECK, true);
        mcm.restoreOneConstraint("test_schema", "car", "value", ConstraintType.DEFAULT, true);
        mcm.restoreOneConstraint("test_schema", "car", "user_id", ConstraintType.DEFAULT, true);
        mcm.restoreOneConstraint("test_schema", "car", "name", ConstraintType.DEFAULT, true);
        mcm.restoreOneConstraint("test_schema", "car", "distfk", ConstraintType.FK, true);
        mcm.restoreOneConstraint("test_schema", "car", "primary", ConstraintType.PK, true);
        mcm.restoreOneConstraint("test_schema", "car", "autoinccol", ConstraintType.NOT_NULL, true);

        tableDDL = jdbcTemplate.queryForObject("SHOW CREATE TABLE test_schema.car;",
                (rs,i)->rs.getString(2));


        String ddlAfterRestoring = "CREATE TABLE `car` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(30) NOT NULL DEFAULT 'BMW',\n" +
                "  `user_name` varchar(30) DEFAULT NULL,\n" +
                "  `user_id` int(11) DEFAULT NULL,\n" +
                "  `value` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autointf` int(11) NOT NULL DEFAULT 5,\n" +
                "  `autoinccol` int(11) NOT NULL DEFAULT 6,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `car_idx` (`name`),\n" +
                "  UNIQUE KEY `unique_name` (`name`,`value`),\n" +
                "  KEY `distfk` (`user_id`,`user_name`),\n" +
                "  KEY `car_upper_idx` (`value`),\n" +
                "  CONSTRAINT `distfk` FOREIGN KEY (`user_id`, `user_name`) REFERENCES `public`.`users` (`id`, `name`) ON DELETE CASCADE ON UPDATE CASCADE,\n" +
                "  CONSTRAINT `car_check` CHECK (`value` > 50 and `autointf` < 100)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        Assert.assertTrue(tableDDL.equals(ddlAfterRestoring));

    }

    DataSource dataSource(){
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUrl("jdbc:mariadb://localhost:3307/test_schema");
        ds.setUsername("root");
        ds.setPassword("root");
        return ds;
    }
}
