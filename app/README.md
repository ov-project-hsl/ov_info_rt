Docker compose zorgt voor een PGADMIN en POSTGRES/timescaledb/postgis database

ov_info_rt
De connect naar de database is hard coded.
Er staan 2 scripts op het image, dus de docker runnen wij met
  docker run -i -t landscapeov/ov_info_rt /bin/bash

Vervolgens starten we hierin eerst de createstatement d.m.v.

  python3 createstatementsv1.py

en hierna

  python3 ETLv1.py
