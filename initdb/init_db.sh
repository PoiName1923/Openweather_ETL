#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE weather_database;
    
    \c airflow
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    
    \c weather_database
    $(cat /docker-entrypoint-initdb.d/weather-schema.sql)
EOSQL