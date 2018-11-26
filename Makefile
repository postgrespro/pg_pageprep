MODULE_big = pg_pageprep
EXTENSION = pg_pageprep
DATA = pg_pageprep--0.1.sql

OBJS = pg_pageprep.o
REGRESS = simple toast
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add

ifdef USE_PGXS
ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

MK_PGPRO_EDITION = $(shell $(PG_CONFIG) --pgpro-edition 2>/dev/null)
ifeq (${MK_PGPRO_EDITION},enterprise)
REGRESS = enterprise
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf-ent.add
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_pageprep
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ISOLATIONCHECKS=updates

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

isolationcheck: | submake-isolation
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
		--temp-config=$(top_srcdir)/$(subdir)/conf.add \
		--outputdir=./isolation_output \
		$(ISOLATIONCHECKS)
