#!/bin/bash

# to run bash script, in terminal cd into the project root and run:
    # permissions - "chmod +x dev-tools/run_sql.sh"
    # then to run use - "./dev-tools/run_sql.sh"

# connect to db
HOST="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
PORT="5432"
USER="project_team_09"
DBNAME="totesys"

echo "You will be prompted to input the TOTESYS database password for user '$USER' ..."

# psql -h $HOST -p $PORT -U $USER -d $DBNAME -f dev-tools/sql/totesys_queries.sql

for file in ./dev-tools/sql/*.sql; do
    psql -h $HOST -p $PORT -U $USER -d $DBNAME -f "${file}" > ${file%.sql}.txt
done
