/* TODO: add @extschema@ */

create type pg_pageprep_status as enum (
	'new',
	'in progress',
	'interrupted',
	'failed',
	'done'
);

create table pg_pageprep_jobs (
	rel			regclass,			/* relation */
	fillfactor	integer,			/* original fillfactor value */
	status		pg_pageprep_status	/* processing status: new, in process, done, failed */
);

create unique index pg_pageprep_data_idx on pg_pageprep_jobs (rel);

create or replace function scan_pages(
	rel			regclass,
	bgworker	boolean default false
)
returns void as 'MODULE_PATHNAME', 'scan_pages'
language c strict;

create or replace function start_bgworker()
returns void as 'MODULE_PATHNAME', 'start_bgworker'
language c strict;

create or replace function stop_bgworker()
returns void as 'MODULE_PATHNAME', 'stop_bgworker'
language c strict;

create view pg_pageprep_todo as (
	select c.oid, c.relname, p.status
	from pg_class c
	left join @extschema@.pg_pageprep_jobs p on p.rel = c.oid
	where
		relkind = 'r' and
		c.oid >= 16384 and
		(status IS NULL OR status != 'done')
);

create or replace function __add_job(rel regclass, fillfactor integer)
returns void as
$$
	insert into @extschema@.pg_pageprep_jobs
	values (rel, fillfactor, 'new')
	on conflict (rel) do nothing;
$$
language sql;


create or replace function __update_status(rel regclass, status text)
returns void as
$$
	update pg_pageprep_jobs
	set status = $2::pg_pageprep_status
	where rel = $1;
$$
language sql;

create or replace function __restore_fillfactors()
returns void as
$$
declare
	row record;
begin
	for row in (select * from pg_pageprep_jobs)
	loop
		execute format('alter table %s set (fillfactor = %s)',
			row.rel, row.fillfactor);
	end loop;
end	
$$
language plpgsql;
