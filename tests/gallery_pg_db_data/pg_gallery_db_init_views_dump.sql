DROP VIEW IF EXISTS ivoa.obscore_view;

CREATE VIEW ivoa.obscore_view AS
    SELECT * FROM ivoa.obscore;

COMMENT ON VIEW ivoa.obscore_view IS 'This is the view of the data_products of the gallery';
