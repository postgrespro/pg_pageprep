/* minimal delays */
SET pg_pageprep.per_relation_delay=0;
SET pg_pageprep.per_page_delay=0;

CREATE FUNCTION show_reloptions(rel regclass) RETURNS text[] AS
$$
	SELECT reloptions FROM pg_class WHERE oid = rel;
$$ LANGUAGE sql;

create extension pg_pageprep;
create table test1(a int) with (fillfactor=80);

-- insert fake value
insert into pg_pageprep_jobs values (
	'test1'::regclass, 50, 'done', 1
);

select show_reloptions('test1'::regclass);
select restore_fillfactors();
select show_reloptions('test1'::regclass);
