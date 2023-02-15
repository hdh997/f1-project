-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step1 - Create circuits table

-- COMMAND ----------

--DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS(path "/mnt/myformula1projectdl/raw/circuits.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step2 - Create races table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitID INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS(path "/mnt/myformula1projectdl/raw/races.csv", header true);

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step3 - Create constructors table
-- MAGIC - Single pline JSON
-- MAGIC - Simple structure 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS(path "/mnt/myformula1projectdl/raw/constructors.json");

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step4 - Create drivers table
-- MAGIC - Single pline JSON
-- MAGIC - Complex structure 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
url STRING
)
USING json
OPTIONS(path "/mnt/myformula1projectdl/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step5 - Create results table
-- MAGIC - Single pline JSON
-- MAGIC - Simple structure 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points FLOAT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId INT
)
USING json
OPTIONS(path "/mnt/myformula1projectdl/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step6 - Create pitstops table
-- MAGIC - multi line JSON
-- MAGIC - Simple structure 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
raceId INT,
stop INT,
time STRING
)
USING json
OPTIONS(path "/mnt/myformula1projectdl/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### List of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step7 - Create lap time table
-- MAGIC - CSV file
-- MAGIC - Multi Files 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS(path "/mnt/myformula1projectdl/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Step8 - Create qualify table
-- MAGIC - JSON file
-- MAGIC - Multi Files 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT
)
USING json
OPTIONS(path "/mnt/myformula1projectdl/raw/qualifying", multiLine True)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------


