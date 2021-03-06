\set verbosity terse

CREATE USER test SUPERUSER LOGIN;
CREATE DATABASE pg_pageprep_regression;
\set prevdb :DBNAME
\c pg_pageprep_regression

/* should be off for testing */
SHOW pg_pageprep.enable_workers;

/* minimal delays */
SET pg_pageprep.per_relation_delay=0;
SET pg_pageprep.per_page_delay=0;

CREATE FUNCTION show_reloptions(rel regclass) RETURNS text[] AS
$$
	SELECT reloptions FROM pg_class WHERE oid = rel;
$$ LANGUAGE sql;

CREATE EXTENSION pg_pageprep;
CREATE VIEW todo_list AS
	SELECT regexp_replace(relname::text, '\d+'::text, '0') as rel1, status
	FROM pg_pageprep_todo
	ORDER BY relname;
CREATE VIEW jobs_list AS
	SELECT regexp_replace(rel::text, '\d+'::text, '0') as rel1, fillfactor,
		status, updated, can_upgrade_table(rel)
	FROM pg_pageprep_jobs
	ORDER BY rel;

CREATE TABLE one(a INT4) WITH (fillfactor=100);
SELECT * FROM todo_list;
INSERT INTO one SELECT i FROM generate_series(1, 1000) i;
SELECT show_reloptions('one');
SELECT scan_pages('one'::regclass);
SELECT show_reloptions('one');
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
SELECT start_bgworker(true);

DROP TABLE two CASCADE;

CREATE TABLE three(a tsvector) WITH (fillfactor=100);
INSERT INTO three SELECT 'a:1 b:2 c:3'::tsvector FROM generate_series(1, 1000) i;
CREATE MATERIALIZED VIEW view_three AS SELECT * FROM three;
SELECT * FROM todo_list;

/* should scan 'three' and 'view_three' */
SELECT start_bgworker(true);
SELECT * FROM todo_list;
SELECT * FROM jobs_list;

DROP TABLE three CASCADE;

DROP VIEW todo_list;
DROP VIEW jobs_list;
DROP EXTENSION pg_pageprep;

\c :prevdb
DROP DATABASE pg_pageprep_regression;
DROP USER test;
