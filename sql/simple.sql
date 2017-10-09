\set verbosity terse

/* should be off for testing */
SHOW pg_pageprep.enable_workers;

/* minimal delays */
SET pg_pageprep.per_relation_delay=1;
SET pg_pageprep.per_page_delay=1;

CREATE EXTENSION pg_pageprep;
CREATE VIEW todo_list AS
	SELECT regexp_replace(relname::text, '\d+'::text, '0') as relname, status
	FROM pg_pageprep_todo
	ORDER BY relname;
CREATE VIEW jobs_list AS
	SELECT regexp_replace(rel::text, '\d+'::text, '0') as rel, fillfactor, status
	FROM pg_pageprep_jobs
	ORDER BY rel;

CREATE TABLE one(a INT4) WITH (fillfactor=100);
SELECT * FROM todo_list;
INSERT INTO one SELECT i FROM generate_series(1, 1000) i;
\d+ one
SELECT scan_pages('one'::regclass);
\d+ one
SELECT * FROM todo_list;
SELECT * FROM jobs_list;

/* should be zero updated */
SELECT scan_pages('one'::regclass);

DROP TABLE one CASCADE;

CREATE TABLE two(a tsvector) WITH (fillfactor=100);
INSERT INTO two SELECT 'a:1 b:2 c:3'::tsvector FROM generate_series(1, 1000) i;
CREATE MATERIALIZED VIEW view_two AS SELECT * FROM two;
SELECT * FROM todo_list;
SELECT scan_pages('two'::REGCLASS);
SELECT scan_pages('view_two'::REGCLASS);
SELECT * FROM todo_list;
SELECT * FROM jobs_list;

/* should be zeros */
SELECT scan_pages('two'::REGCLASS);
SELECT scan_pages('view_two'::REGCLASS);
INSERT INTO two SELECT 'a:1 b:2 c:3'::tsvector FROM generate_series(1, 1000) i;

/* should be still zero */
SELECT scan_pages('two'::REGCLASS);

/* nothing to do */
SELECT start_bgworker();

DROP TABLE two CASCADE;

CREATE TABLE two(a tsvector) WITH (fillfactor=100);
INSERT INTO two SELECT 'a:1 b:2 c:3'::tsvector FROM generate_series(1, 1000) i;
CREATE MATERIALIZED VIEW view_two AS SELECT * FROM two;
SELECT * FROM todo_list;

/* should scan 'two' and 'view_two' */
SELECT start_bgworker();
SELECT * FROM todo_list;
SELECT * FROM jobs_list;

DROP VIEW todo_list;
DROP VIEW jobs_list;
