# pg_pageprep

**WIP**

`pg_pageprep` is an extension that is supposed to help to prepare heap pages for migration to 64bit XID page format.

PostgresPro Enterprise page format reqiures extra 24 bytes per page compared to original PostgreSQL in order to support 64bit transaction IDs. The idea behind this extension is to prepare enough space in pages for new format while database is working on vanilla postgres.

# Installation

```
make install USE_PGXS=1
```

or if postgres binaries are not on PATH

```
make install USE_PGXS=1 PG_CONFIG=/path/to/pg_config
```

After that perform `CREATE EXTENSION pg_pageprep;` on each database in cluster.

# Configuration

You can add the following parameters to your postgres config:

* `pg_pageprep.databases` - comma separated list of databases (default 'postgres');
* `pg_pageprep.role` - user name (default 'postgres');
* `pg_pageprep.per_page_delay` - delay between consequent page scans in milliseconds (default 100ms);
* `pg_pageprep.per_relation_delay` - delay b/w relations scans in milliseconds (default 1000ms);
* `pg_pageprep.per_attempt_delay` - delay b/w attempts to scan next relation in case previous attempt was unsuccessful, e.g. there are all relations are already done (default 60s).
