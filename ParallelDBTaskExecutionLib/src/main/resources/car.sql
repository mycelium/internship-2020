CREATE SCHEMA IF NOT EXISTS test_schema;

drop table if exists public.users2

create table users2
(
	id integer not null,
	name varchar(255) not null,
	constraint postgrespk
		primary key (id, name)
);

drop table if exists test_schema.car

create table test_schema.car
(
	id integer not null
		constraint car_pkey
			primary key,
	name varchar(30) default 'BMW' not null
		constraint unique_name
			unique,
	user_name varchar(30),
	user_id integer,
	value integer default 5 not null,
	autointf integer default 5 not null,
	autoinccol integer default 6 not null,
	constraint distfk
		foreign key (user_id, user_name) references users2
			on update cascade on delete cascade,
	constraint car_check
		check ((value > 50) AND (autointf < 100))
);

CREATE INDEX car_upper_idx ON test_schema.car ((upper(name)));
CREATE UNIQUE INDEX car_idx ON test_schema.car (name);