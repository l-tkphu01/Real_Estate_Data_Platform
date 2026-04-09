FROM apache/superset:4.1.1

# Switch to root to install packages
USER root

# Install DuckDB and the SQLAlchemy engine for DuckDB
RUN pip install --no-cache-dir duckdb==1.1.3 duckdb-engine==0.15.0

# Switch back to the default superset user
USER superset
