CREATE SCHEMA ivoa;

ALTER SCHEMA ivoa OWNER TO postgres;

SET search_path = ivoa, public;

-- TODO download and install the extension via the github actions
--CREATE EXTENSION IF NOT EXISTS pg_sphere WITH SCHEMA ivoa;

CREATE TABLE ivoa.obscore (
    title character varying(255) NOT NULL,
    product_path character varying(255) DEFAULT NULL::character varying,
    e1_kev double precision NOT NULL,
    e2_kev double precision NOT NULL,
    ra double precision NOT NULL,
    "dec" double precision NOT NULL,
    time_bin double precision NOT NULL,
    instrument_name character varying(255) NOT NULL,
    product_type_name character varying(255) NOT NULL,
    t_start character varying(20) NOT NULL,
    t_stop character varying(20) NOT NULL,
    proposal_id character varying(255) NOT NULL,
    sources character varying(300) NOT NULL,
    access_url character varying(255) NOT NULL,
    image_uri character varying(255) NOT NULL
);
-- dummy description for the table and each column
COMMENT ON TABLE ivoa.obscore IS 'This is the table of the data_products of the gallery.';

COMMENT ON COLUMN ivoa.obscore.title IS 'The title of the data product.';
COMMENT ON COLUMN ivoa.obscore.product_path IS 'The path to the data product.';
COMMENT ON COLUMN ivoa.obscore.e1_kev IS 'This is e1';
COMMENT ON COLUMN ivoa.obscore.e2_kev IS 'This is e2';
COMMENT ON COLUMN ivoa.obscore.ra IS 'This is the RA';
COMMENT ON COLUMN ivoa.obscore.dec IS 'This is the DEC';
COMMENT ON COLUMN ivoa.obscore.time_bin IS 'This is the time bin';
COMMENT ON COLUMN ivoa.obscore.instrument_name IS 'This is the mame of the instrument';
COMMENT ON COLUMN ivoa.obscore.product_type_name IS 'This is the name of the product type';
COMMENT ON COLUMN ivoa.obscore.t_start IS 'This is the start time';
COMMENT ON COLUMN ivoa.obscore.t_stop IS 'This is the end time';
COMMENT ON COLUMN ivoa.obscore.proposal_id IS 'This is the proposal id';
COMMENT ON COLUMN ivoa.obscore.sources IS 'This is the source';
COMMENT ON COLUMN ivoa.obscore.access_url IS 'This is the uri of the file';
COMMENT ON COLUMN ivoa.obscore.image_uri IS 'This is the uri of the image';

ALTER TABLE ivoa.obscore OWNER TO postgres;
