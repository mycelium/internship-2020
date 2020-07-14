-- Table: public.users2

-- DROP TABLE public.users2;

CREATE TABLE public.users2
(
    id integer NOT NULL,
    name character varying(255) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT postgrespk PRIMARY KEY (id, name)
)

TABLESPACE pg_default;

ALTER TABLE public.users2
    OWNER to postgres;

CREATE TABLE test_schema.car2
(
    id integer NOT NULL,
    name character varying(30) COLLATE pg_catalog."default" NOT NULL DEFAULT 'BMW'::character varying,
    user_name character varying(30) COLLATE pg_catalog."default",
    user_id integer,
    value integer NOT NULL DEFAULT 5,
    autointf integer NOT NULL DEFAULT 5,
    CONSTRAINT car_pkey2 PRIMARY KEY (id),
    CONSTRAINT unique_name2 UNIQUE (name),
    CONSTRAINT distfk2 FOREIGN KEY (user_id, user_name)
        REFERENCES public.users2 (id, name) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT car_check2 CHECK (value > 50 AND autointf < 100)
)

    TABLESPACE pg_default;

ALTER TABLE test_schema.car2
    OWNER to postgres;
-- Index: car_idx

-- DROP INDEX public.car_idx;

CREATE UNIQUE INDEX car_idx2
    ON test_schema.car2 USING btree
    (name COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: car_upper_idx

-- DROP INDEX public.car_upper_idx;

CREATE INDEX car_upper_idx2
    ON test_schema.car2 USING btree
    (upper(user_name::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;


-- Table: test_schema.car

-- DROP TABLE test_schema.car;

CREATE TABLE test_schema.car
(
    id integer NOT NULL,
    name character varying(30) COLLATE pg_catalog."default" NOT NULL DEFAULT 'BMW'::character varying,
    user_name character varying(30) COLLATE pg_catalog."default",
    user_id integer,
    value integer NOT NULL DEFAULT 5,
    autointf integer NOT NULL DEFAULT 5,
    CONSTRAINT distfk FOREIGN KEY (user_id, user_name)
        REFERENCES public.users2 (id, name) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)

TABLESPACE pg_default;

ALTER TABLE test_schema.car
    OWNER to postgres;
-- Index: car_upper_idx

-- DROP INDEX test_schema.car_upper_idx;

CREATE INDEX car_upper_idx
    ON test_schema.car USING btree
    (upper(user_name::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;