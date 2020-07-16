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
    ADD CONSTRAINT unique_name  UNIQUE (name);
ALTER TABLE test_schema.car
    ADD PRIMARY KEY (id);

CREATE INDEX car_upper_idx ON test_schema.car (value);
CREATE UNIQUE INDEX car_idx ON test_schema.car (name);
