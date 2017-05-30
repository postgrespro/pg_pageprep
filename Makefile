MODULE_big = pg_pageprep
EXTENSION = pg_pageprep
DATA = pg_pageprep--0.1.sql

OBJS = pg_pageprep.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cube
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif