create table pg_pageprep_jobs (
	rel			regclass,			/* relation */
	fillfactor	integer,			/* original fillfactor value,
									 * NULL if not set */
	status		text,				/* 'done' means done */
	updated		integer
) with (fillfactor=90);
create unique index pg_pageprep_data_idx on pg_pageprep_jobs (rel);

create or replace function pg_pageprep_event_trigger()
returns event_trigger as
$$
begin
    delete from @extschema@.pg_pageprep_jobs
    where rel in (
        select objid
        from pg_event_trigger_dropped_objects()
        where classid = 'pg_class'::regclass
    );
end
$$
language plpgsql;

create event trigger
pg_pageprep_event_trigger
on sql_drop
execute procedure pg_pageprep_event_trigger();

create or replace function scan_pages(
	rel			regclass
)
returns void as 'MODULE_PATHNAME', 'scan_pages_pl'
language c strict;


create or replace function start_bgworker(wait boolean default false)
returns void as 'MODULE_PATHNAME', 'start_bgworker'
language c strict;


create or replace function stop_bgworker()
returns void as 'MODULE_PATHNAME', 'stop_bgworker'
language c strict;


create or replace function estimate_time()
returns bigint as 'MODULE_PATHNAME', 'estimate_time'
language c strict;


create or replace function get_workers_list()
returns table (pid integer, database text, status text)
as 'MODULE_PATHNAME', 'get_workers_list'
language c strict;

create or replace function can_upgrade_table(rel regclass)
returns bool as 'MODULE_PATHNAME', 'can_upgrade_table'
language c strict;


create view pg_pageprep_todo as (
	select c.oid, c.relname, p.status
	from pg_class c
	left join @extschema@.pg_pageprep_jobs p on p.rel = c.oid
	where
		relkind in ('r', 't', 'm') and
		relname != 'pg_pageprep_jobs' and
		relpersistence != 't' and
		c.oid >= 16384 and
		(status IS NULL OR status != 'done')
	order by c.relname
);

create or replace function __add_job(rel regclass)
returns void as
$$
declare
	did_insert bool = null;
begin
	with relinfo as (
		select fillfactor
		from (select reloptions from pg_class where oid = rel) r
		left join lateral (select option_value::int fillfactor
			from pg_options_to_table(reloptions)
			where option_name = 'fillfactor') o
		on true
	),
	new_job as (
		insert into pg_pageprep_jobs
		select rel, fillfactor, 'new' from relinfo
		on conflict do nothing returning true
	)
	select true from new_job, relinfo into did_insert;

	-- Don't update fillfactor if we didn't create the job.
	if did_insert then
		perform __set_fillfactor(rel, 90);
		raise notice 'fillfactor was updated for "%"', rel;
	end if;
end
$$
language plpgsql;

--
-- Set fillfactor for the relation. Pass fillfactor=NULL to remove the option.
--
-- Before updating the pages, we set fillfactor to lower than 100%, so that
-- the subsequent updates don't make the pages run out of space again.
-- Fillfactor of normal heap tables can be changed with ALTER TABLE SET
-- (fillfactor), but there is no corresponding storage parameter for TOAST
-- tables. For these tables, the fillfactor is updated at runtime using a
-- relcache hook (see set_relcache_fillfactor). For this hook to work, the
-- rd_options must be initialized. This, in turn, requires that the relation
-- has some non-default options in pg_class.reloptions.
-- For normal tables, we could use ALTER TABLE SET (fillfactor), but for
-- the sake of uniformity, we parse pg_class.reloptions manually and add
-- 'fillfactor=<num>' for all kinds of tables. This both enables the runtime
-- fillfactor hook for TOAST tables, and directly sets the fillfactor for
-- normal tables.
--
create or replace function __set_fillfactor(relid oid, fillfactor integer)
returns void
as
$$
declare
    row      record;
    relopts  text[];
    relopt   text;
    new_relopts text[];
    found    bool := false;
	fillfactor_opt text[] = array[]::text[];
begin
	if fillfactor is not null then
		fillfactor_opt = array[format('fillfactor=%s', fillfactor)];
	end if;

    for relopts in select reloptions from pg_class where oid = relid for update
    loop
        for relopt in select * from unnest(relopts)
        loop
            if position('fillfactor' in relopt) > 0 then
				new_relopts := new_relopts || fillfactor_opt;
                found := true;
            else
                new_relopts := new_relopts || relopt;
            end if;
        end loop;
    end loop;

    if not found then
		new_relopts := new_relopts || fillfactor_opt;
    end if;

	if cardinality(new_relopts) = 0 then
		new_relopts = null;
	end if;

    update pg_class set reloptions = new_relopts where oid = relid;
end
$$
language plpgsql;


create or replace function restore_fillfactors()
returns void as
$$
declare
	row record;
begin
	for row in (select rel, fillfactor
				from @extschema@.pg_pageprep_jobs p
				join pg_class c on c.oid = p.rel and c.relkind != 't')
	loop
		perform __set_fillfactor(row.rel, row.fillfactor);
	end loop;
end
$$
language plpgsql;
