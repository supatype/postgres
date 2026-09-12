# Keys-only logical decoding output plugin for the Mode B row cache.
MODULES = supacache_keys

# A server-version mismatch in this plugin is a segfault in whatever backend is
# decoding, not a failed build: reading PG16's ReorderBufferTupleBuf as a
# HeapTuple compiled with three warnings and crash-looped the cluster (#90).
# The compiler diagnosed it correctly and nothing was listening, so this class
# of mistake is an error here rather than a warning.
PG_CPPFLAGS += -Werror=incompatible-pointer-types

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
