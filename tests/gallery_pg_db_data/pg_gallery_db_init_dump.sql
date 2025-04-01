CREATE SCHEMA mmoda_pg_dev;

ALTER SCHEMA mmoda_pg_dev OWNER TO postgres;

SET search_path = mmoda_pg_dev, public;

-- TODO download and install the extension via the github actions
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
    t_start character varying(20) NOT NULL,
    t_stop character varying(20) NOT NULL,
    proposal_id character varying(255) NOT NULL,
    sources character varying(300) NOT NULL,
    file_name character varying(255) DEFAULT NULL::character varying,
    file_uri character varying(255) NOT NULL,
    image_name character varying(255) DEFAULT NULL::character varying,
    image_uri character varying(255) NOT NULL
);
-- dummy description for the table and each column
COMMENT ON TABLE mmoda_pg_dev.data_product_table_view_v IS 'This is the table of the data_products of the gallery.';

COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.title IS 'The title of the data product.';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.product_path IS 'The path to the data product.';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.e1_kev IS 'This is e1';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.e2_kev IS 'This is e2';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.ra IS 'This is the RA';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.dec IS 'This is the DEC';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.time_bin IS 'This is the time bin';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.instrument_name IS 'This is the mame of the instrument';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.product_type_name IS 'This is the name of the product type';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.t_start IS 'This is the start time';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.t_stop IS 'This is the end time';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.proposal_id IS 'This is the proposal id';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.sources IS 'This is the source';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.file_name IS 'This is the name of the file';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.file_uri IS 'This is the uri of the file';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.image_name IS 'This is the name of the image';
COMMENT ON COLUMN mmoda_pg_dev.data_product_table_view_v.image_uri IS 'This is the uri of the image';

ALTER TABLE mmoda_pg_dev.data_product_table_view_v OWNER TO postgres;
