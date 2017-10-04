\set verbosity terse
CREATE EXTENSION pg_pageprep;
CREATE TABLE one(a INT4) WITH (fillfactor=90);
INSERT INTO one SELECT FROM generate_series(1, 2000 * 10);
\d+ one
SELECT scan_pages();
SELECT pg_sleep(10);
