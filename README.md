# Simple anomaly detector via PySpark

**Tiny description:**

  This program contains algorithm of distributed detection anomalies in such user identifiers as IDFA, GAID, email, login in map-reduce paradigm.


**How to run:**

Main module – anomaly_detection/main.py

Need to export: SPARK_MASTER, DATA_DIR, EDGES_TABLE_NAME

where

DATA_DIR – full path to directory with stored tables with data, 
EDGES_TABLE_NAME – just name of table where you store edges data.
