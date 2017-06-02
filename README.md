# pg_pageprep

**WIP**

`pg_pageprep` is an extension that is supposed to help to prepare heap pages for migration to 64bit XID page format.

PostgresPro Enterprise page format reqiures extra 16 bytes per page comparing to original PostgreSQL in order to support 64bit transaction IDs. The idea behind this extension is to prepare enough space in pages for new format while database is working on vanilla postgres.