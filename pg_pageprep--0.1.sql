/* TODO: add @extschema@ */

create table pg_pageprep_data (
	rel			regclass,	/* relation */
	fillfactor	integer,	/* original fillfactor value */
	status		integer		/* processing status: new, in process, done, failed */
);

create unique index pg_pageprep_data_idx on pg_pageprep_data (rel);

create or replace function scan_pages(
	rel			regclass,
	bgworker	boolean default false
)
returns void as 'MODULE_PATHNAME', 'scan_pages'
language c strict;
