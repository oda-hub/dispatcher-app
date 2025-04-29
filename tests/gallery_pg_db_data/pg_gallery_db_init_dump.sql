CREATE SCHEMA ivoa;

ALTER SCHEMA ivoa OWNER TO postgres;

SET search_path = ivoa, public;

-- TODO download and install the extension via the github actions
--CREATE EXTENSION IF NOT EXISTS pg_sphere WITH SCHEMA ivoa;

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
    t_min character varying(20),
    t_max character varying(20),
    proposal_id character varying(255),
    target_name character varying(300),
    access_url character varying(255),
    image_uri character varying(255)
);
-- dummy description for the table and each column
COMMENT ON TABLE ivoa.obscore IS 'This is the table of the data_products of the gallery.';

COMMENT ON COLUMN ivoa.obscore.obs_title IS 'The title of the data product.';
COMMENT ON COLUMN ivoa.obscore.product_path IS 'The path to the data product.';
COMMENT ON COLUMN ivoa.obscore.em_min IS 'This is e1';
COMMENT ON COLUMN ivoa.obscore.em_max IS 'This is e2';
COMMENT ON COLUMN ivoa.obscore.s_ra IS 'This is the RA';
COMMENT ON COLUMN ivoa.obscore.s_dec IS 'This is the DEC';
COMMENT ON COLUMN ivoa.obscore.time_bin IS 'This is the time bin';
COMMENT ON COLUMN ivoa.obscore.instrument_name IS 'This is the mame of the instrument';
COMMENT ON COLUMN ivoa.obscore.dataproduct_type IS 'This is the name of the product type';
COMMENT ON COLUMN ivoa.obscore.t_min IS 'This is the start time';
COMMENT ON COLUMN ivoa.obscore.t_max IS 'This is the end time';
COMMENT ON COLUMN ivoa.obscore.proposal_id IS 'This is the proposal id';
COMMENT ON COLUMN ivoa.obscore.target_name IS 'This is the source';
COMMENT ON COLUMN ivoa.obscore.access_url IS 'This is the uri of the file';
COMMENT ON COLUMN ivoa.obscore.image_uri IS 'This is the uri of the image';

ALTER TABLE ivoa.obscore OWNER TO postgres;
