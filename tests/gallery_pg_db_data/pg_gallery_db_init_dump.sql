CREATE SCHEMA IF NOT EXISTS ivoa;

ALTER SCHEMA ivoa OWNER TO postgres;

SET search_path = ivoa, public;

CREATE EXTENSION IF NOT EXISTS pg_sphere WITH SCHEMA ivoa;

CREATE TABLE ivoa.obscore (
    obs_title character varying(255) NOT NULL,
    product_path character varying(255) DEFAULT NULL::character varying,
    em_min double precision,
    em_max double precision,
    s_ra double precision,
    s_dec double precision,
    time_bin double precision,
    instrument_name character varying(255),
    dataproduct_type character varying(255),
    t_min double precision,
    t_max double precision,
    proposal_id character varying(255),
    target_name character varying(300),
    access_url character varying(255),
    image_uri character varying(255)
);
-- dummy description for the table and each column
COMMENT ON TABLE ivoa.obscore IS 'This is the table of the data_products of the gallery';

COMMENT ON COLUMN ivoa.obscore.obs_title IS 'obs_title of the data product';
COMMENT ON COLUMN ivoa.obscore.product_path IS 'product_path of the data product';
COMMENT ON COLUMN ivoa.obscore.em_min IS 'em_min of the data product';
COMMENT ON COLUMN ivoa.obscore.em_max IS 'em_max of the data product';
COMMENT ON COLUMN ivoa.obscore.s_ra IS 's_ra of the data product';
COMMENT ON COLUMN ivoa.obscore.s_dec IS 's_dec of the data product';
COMMENT ON COLUMN ivoa.obscore.time_bin IS 'time_bin of the data product';
COMMENT ON COLUMN ivoa.obscore.instrument_name IS 'instrument_name of the data product';
COMMENT ON COLUMN ivoa.obscore.dataproduct_type IS 'dataproduct_type of the data product';
COMMENT ON COLUMN ivoa.obscore.t_min IS 't_min of the data product';
COMMENT ON COLUMN ivoa.obscore.t_max IS 't_max of the data product';
COMMENT ON COLUMN ivoa.obscore.proposal_id IS 'proposal_id of the data product';
COMMENT ON COLUMN ivoa.obscore.target_name IS 'target_name of the data product';
COMMENT ON COLUMN ivoa.obscore.access_url IS 'access_url of the data product';
COMMENT ON COLUMN ivoa.obscore.image_uri IS 'image_uri of the data product';

ALTER TABLE ivoa.obscore OWNER TO postgres;
