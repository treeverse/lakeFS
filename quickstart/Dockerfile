FROM --platform=linux/amd64 ubuntu:20.04

RUN apt-get update && apt-get install -y \
    gettext-base \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install DuckDB
# --------------

RUN wget https://github.com/duckdb/duckdb/releases/download/v0.7.1/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-amd64.zip

# Create ~/.duckdbrc file
# -----------------------

RUN echo ".prompt '⚫◗ '" > $HOME/.duckdbrc

# Install Extensions
# ------------------

env EXTENSIONS="fts httpfs icu json parquet postgres_scanner sqlite_scanner substrait"

RUN for e in $EXTENSIONS; do \
    echo "Installing $e ..."; \
    duckdb -c "INSTALL $e;"; \
    echo "LOAD $e;" >> $HOME/.duckdbrc; \
    done

ENTRYPOINT ["/usr/local/bin/duckdb", "-unsigned"]
CMD []
