MODULE_big = pg_pageprep
EXTENSION = pg_pageprep
DATA = pg_pageprep--0.1.sql

OBJS = pg_pageprep.o

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif
MK_PGPRO_EDITION = $(shell $(PG_CONFIG) --pgpro-edition)

ifeq (${MK_PGPRO_EDITION},enterprise)
REGRESS = enterprise
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf_ent.add
else
REGRESS = simple toast
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ISOLATIONCHECKS=updates

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

isolationcheck: | submake-isolation
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
		--temp-config=$(top_srcdir)/$(subdir)/conf.add \
		--outputdir=./isolation_output \
		$(ISOLATIONCHECKS)
