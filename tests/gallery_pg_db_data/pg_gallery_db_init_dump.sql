CREATE SCHEMA mmoda_pg_dev;

ALTER SCHEMA mmoda_pg_dev OWNER TO postgres;

SET search_path = mmoda_pg_dev, public;

--CREATE EXTENSION IF NOT EXISTS pg_sphere WITH SCHEMA mmoda_pg_dev;

CREATE TABLE mmoda_pg_dev.data_product_table_view_v (
    title character varying(255) NOT NULL,
    product_path character varying(255) DEFAULT NULL::character varying,
    e1_kev double precision NOT NULL,
    e2_kev double precision NOT NULL,
    ra double precision NOT NULL,
    "dec" double precision NOT NULL,
    time_bin double precision NOT NULL,
    instrument_name character varying(255) NOT NULL,
    product_type_name character varying(255) NOT NULL,
    rev1 bigint NOT NULL,
    rev2 bigint NOT NULL,
    t_start character varying(20) NOT NULL,
    t_stop character varying(20) NOT NULL,
    proposal_id character varying(255) NOT NULL,
    sources character varying(300) NOT NULL,
    file_name character varying(255) DEFAULT NULL::character varying,
    file_uri character varying(255) NOT NULL,
    image_name character varying(255) DEFAULT NULL::character varying,
    image_uri character varying(255) NOT NULL
);

ALTER TABLE mmoda_pg_dev.data_product_table_view_v OWNER TO postgres;
