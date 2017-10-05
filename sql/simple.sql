\set verbosity terse
SHOW pg_pageprep.enable_workers;
SET pg_pageprep.per_relation_delay=1;
SET pg_pageprep.per_page_delay=1;
SET pg_pageprep.autoscan=off;
CREATE EXTENSION pg_pageprep;
CREATE TABLE one(a INT4) WITH (fillfactor=100);
INSERT INTO one SELECT FROM generate_series(1, 2000 * 10);
\d+ one
SELECT scan_pages('one'::regclass);
\d+ one
