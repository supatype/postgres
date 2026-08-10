# Build/test image for supatype_mask.
#
#   docker build -t supatype-mask-build -f test/Dockerfile test
#   docker run --rm -v "$PWD:/src" -w /src supatype-mask-build bash -c \
#     "make install && chown -R postgres /tmp && \
#      su postgres -c 'SKIP_INSTALL=1 bash test/run-tests.sh supatype_mask'"
#
# Same base as the product image, so a compile failure here is a compile failure there.
FROM postgres:17-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential postgresql-server-dev-17 \
  && rm -rf /var/lib/apt/lists/*
