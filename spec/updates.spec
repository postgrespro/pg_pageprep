setup
{
	create extension pg_pageprep;
	create table one(a tsvector, b int) with (fillfactor=100);
	insert into one select 'a:1 b:2 c:3'::tsvector, i from generate_series(1, 100) i;
}

teardown
{
	drop table one cascade;
	drop extension pg_pageprep;
}

session "s1"
step s1_scan { select scan_pages('one'); }
step s1_jobs { select * from pg_pageprep_jobs; }

session "s2"
step s2_begin	{ begin; }
step s2_su		{ select * from one a < 50 for update; }
step s2_update	{ update one set a=a where a < 50; }
step s2_commit	{ commit; }

permutation "s2_begin" "s1_update" "s1_scan" "s1_jobs" "s2_commit"
permutation "s2_begin" "s2_su" "s1_scan" "s2_update" "s1_jobs" "s2_commit"
permutation "s2_begin" "s1_scan" "s2_update" "s1_jobs" "s2_commit"
