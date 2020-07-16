DROP DATABASE test_schema;

CREATE SCHEMA IF NOT EXISTS test_schema;

drop table if exists test_schema.car;
drop table if exists public.users;

USE test_schema;

create table public.users
(
    id integer not null,
    name varchar(255) not null,
    constraint postgrespk
        primary key (id, name)
);

CREATE TABLE test_schema.car (
                       id int(11) NOT NULL,
                       name varchar(30) NOT NULL DEFAULT 'BMW',
                       user_name varchar(30),
                       user_id int(11) ,
                       value int(11) NOT NULL DEFAULT 5,
                       autointf int(11) NOT NULL DEFAULT 5,
                       autoinccol int(11) NOT NULL DEFAULT 6,
                       KEY distfk (user_id,user_name),
                       CONSTRAINT distfk FOREIGN KEY (user_id, user_name) REFERENCES public.users(id, name) ON DELETE CASCADE ON UPDATE CASCADE,
                       CONSTRAINT car_check CHECK (value > 50 and autointf < 100)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

ALTER TABLE test_schema.car
    ADD CONSTRAINT unique_name  UNIQUE (name, value);
ALTER TABLE test_schema.car
    ADD PRIMARY KEY (id);

CREATE INDEX car_upper_idx ON test_schema.car (value);
CREATE UNIQUE INDEX car_idx ON test_schema.car (name);


drop table if exists public.product_order;
drop table if exists public.customer;
drop table if exists public.product;

CREATE TABLE public.product (
	`category` INT(11) NOT NULL,
	`id` INT(11) NOT NULL,
	`price` DECIMAL(10,0) NULL DEFAULT NULL,
	PRIMARY KEY (`category`, `id`) USING BTREE
)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB
;


CREATE TABLE public.customer (
	`id` INT(11) NOT NULL,
	PRIMARY KEY (`id`) USING BTREE
)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB
;


CREATE TABLE public.product_order (
	`no` INT(11) NOT NULL AUTO_INCREMENT,
	`product_category` INT(11) NOT NULL,
	`product_id` INT(11) NOT NULL,
	`customer_id` INT(11) NOT NULL,
	PRIMARY KEY (`no`) USING BTREE,
	INDEX `product_category` (`product_category`, `product_id`) USING BTREE,
	INDEX `customer_id` (`customer_id`) USING BTREE,
	CONSTRAINT `product_order_ibfk_1` FOREIGN KEY (`product_category`, `product_id`) REFERENCES `public`.`product` (`category`, `id`) ON UPDATE CASCADE ON DELETE RESTRICT,
	CONSTRAINT `product_order_ibfk_2` FOREIGN KEY (`customer_id`) REFERENCES `public`.`customer` (`id`) ON UPDATE RESTRICT ON DELETE RESTRICT
)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB
;

