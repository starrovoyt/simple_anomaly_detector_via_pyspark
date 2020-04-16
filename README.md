# Simple anomaly detector via PySpark

**Tiny description:**

  This program contains algorithm of distributed anomaly detection in such user identifiers as IDFA (iOS advertising ID gererated as uuid), GAID (Android advertising ID gererated as uuid), email, and login in map-reduce paradigm.

**Idea:**
Generate factors, based only on representation of identifier and number of its connections with other identifiers.

**How to run:**

Main module – anomaly_detection/main.py

Need to export: SPARK_MASTER, DATA_DIR, EDGES_TABLE_NAME

where

DATA_DIR – full path to directory with stored tables with data, 
EDGES_TABLE_NAME – just name of table where you store edges data.
