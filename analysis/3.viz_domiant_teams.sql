-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style = "color:black; text-align:center; font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
As
SELECT team_name,
        COUNT(*) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points,
        RANK()OVER(ORDER BY AVG(calculated_points) DESC) AS team_rank
FROM f1_performance.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(*) >= 100 
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year,
        team_name,
        COUNT(*) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
FROM f1_performance.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
GROUP BY race_year,team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year,
        team_name,
        COUNT(*) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
FROM f1_performance.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
GROUP BY race_year,team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year,
        team_name,
        COUNT(*) AS total_races,
        SUM(calculated_points) AS total_points,
        AVG(calculated_points) AS avg_points
FROM f1_performance.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
GROUP BY race_year,team_name
ORDER BY race_year, avg_points DESC
