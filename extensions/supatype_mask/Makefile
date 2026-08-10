MODULE_big = supatype_mask
EXTENSION  = supatype_mask
MODVERSION = 1.0.0
DATA       = supatype_mask--1.0.sql

PG_CONFIG ?= pg_config

SRC_DIR = src
SRC     = $(wildcard $(SRC_DIR)/*.c)
OBJS    = $(patsubst $(SRC_DIR)/%.c, $(SRC_DIR)/%.o, $(SRC))

# gnu11 rather than c11: `copyObject` expands to `typeof`, a GNU extension.
# The `-Wno`s quiet C90 warnings the Postgres headers trip under -Wextra.
PG_CFLAGS = -std=gnu11 -Wextra -Wall -Werror \
	-Wno-declaration-after-statement \
	-Wno-vla \
	-Wno-long-long

PG_CPPFLAGS = -DMODVERSION=\"$(MODVERSION)\"

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-extension=supatype_mask

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: test
test:
	$(MAKE) installcheck
