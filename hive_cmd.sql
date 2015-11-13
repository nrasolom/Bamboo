-- # All commands from the ISEP'15 Big Data Projetc

-- # Hcat is used to creat the external table
-- # load the whole folder of files
LOAD DATA INPATH '/user/panda/enernoc/' OVERWRITE INTO TABLE panda.all_records;

-- # ======== fillin time: s / bucket weight: / partion weight:

-- # depending of the scope of the studied we fill the work table

-- #######################################################################################
CREATE TABLE tibet.work_table_site_axis(
  datetime STRING,
  values ARRAY<FLOAT>
)
COMMENT 'Work table, values containe all sites measures for the same datetime.'
PARTITIONED BY (season STRING)
CLUSTERED BY(datetime) INTO 3 BUCKETS
ROW FORMAT DELIMITED   
FIELDS TERMINATED BY ','
STORED AS ORC;

-- # 

FROM view_work_table
INSERT OVERWRITE TABLE work_table_site_axis PARTITION (season)
SELECT dttm_utc AS datetime,
COLLECT_LIST(value) AS values
season(dttm_utc) AS season
GROUP BY dttm_utc ORDER BY dttm_utc;

-- ########### fillin time: 24:51s / lines: 105407 / bucket weight: / partion weight:

-- -------------------------------------------------------------------- ANSWER: QUESTION 1

SELECT datetime AS datetime, arraysum(values) AS total
FROM work_table_site_axis 
ORDER BY datetime;

-- time: 2:08s
-- stage: 1
-- nbr of lines: 105407 
|	datetime	|	total	|
---------------- -------------
|2015-01-01 00:	|	*****	|
|2015-01-01 01:	|	*****	|
|2015-01-01 02:	|	*****	|
-- ---------------------------------------------------------------------------------------

-- -------------------------------------------------------------------- ANSWER: QUESTION 3

SELECT getweek(datetime) AS week, SUM(arraysum(values)) AS total
FROM work_table_site_axis
GROUP BY getweek(datetime)
ORDER BY week;

-- time: 2:12s
-- stage: 1
-- nbr of lines: 105407 
|	datetime	|	total	|
---------------- -------------
|weeek nbr n	|	*****	|
|weeek nbr n+1	|	*****	|
|weeek nbr n+2	|	*****	|
-- ---------------------------------------------------------------------------------------

-- #######################################################################################

CREATE TABLE tibet.work_table_industry_axis(
  datetime STRING,
  values ARRAY<FLOAT>
)
COMMENT 'Work table, values containe all sites measures for the same industry,datetime.'
PARTITIONED BY (industry STRING)
CLUSTERED BY(datetime) INTO 25 BUCKETS
ROW FORMAT DELIMITED   
FIELDS TERMINATED BY ','
STORED AS ORC;

-- # fill the time based axis table

FROM view_work_table
INSERT OVERWRITE TABLE work_table_industry_axis PARTITION (industry)
SELECT dttm_utc AS dttm_utc,
COLLECT_LIST(value) AS values, 
industry AS industry
GROUP BY dttm_utc, industry  ORDER BY dttm_utc;

-- ######## fillin time: 38m88s / lines: 105407*4 / bucket weight: / partion weight:

-- -------------------------------------------------------------------- ANSWER: QUESTION 2
SELECT datetime AS datetime, industry AS sector, arrayavg(values) AS total
FROM work_table_industry_axis
ORDER BY datetime, sector;

-- time: 3:52s
-- stage: 2
-- nbr of lines: 105407 * 4
|	datetime	| 	sector 	|	total	|
---------------- -------------
|2015-01-01 00:	| Education	|	*****	|
|2015-01-01 01:	| Education	|	*****	|
|2015-01-01 02:	| Education	|	*****	|
-- ---------------------------------------------------------------------------------------

-- -------------------------------------------------------------------- ANSWER: QUESTION 4
SELECT getweek(datetime) AS week, industry, AVG(arrayavg(values)) AS total
FROM work_table_industry_axis
GROUP BY industry, getweek(datetime)
ORDER BY week; 

-- time: 3:58s
-- stage: 1
-- nbr of lines: 105407 
|	datetime	|	mean	|
---------------- -------------
|weeek nbr n	|	*****	|
|weeek nbr n+1	|	*****	|
|weeek nbr n+2	|	*****	|
-- ---------------------------------------------------------------------------------------


CREATE TABLE tibet.work_table_day_axis(
  date STRING,
  season STRING,
  value FLOAT,
  site_id STRING,
  sub_industry STRING, 
  sq_ft BIGINT,
  lat FLOAT,
  lng FLOAT 
)
COMMENT 'Work table, value col correspond to one day/site.'
PARTITIONED BY (industry STRING)
CLUSTERED BY(site_id) INTO 25 BUCKETS
ROW FORMAT DELIMITED   
FIELDS TERMINATED BY ','
STORED AS ORC;


FROM all_sites JOIN ( SELECT
  to_date(dttm_utc) AS date,
  SUM(value) AS value, filename(INPUT__FILE__NAME) AS site_id FROM all_records
  GROUP BY filename(INPUT__FILE__NAME), dttm_utc ORDER BY date
  ) 
AS tab ON tab.site_id  = all_sites.site_id
INSERT OVERWRITE TABLE work_table_day_axis PARTITION (industry)
SELECT tab.date AS date,
season(tab.date) AS season,
tab.value AS value,
tab.site_id AS site_id,
all_sites.sub_industry AS sub_industry, 
all_sites.sq_ft AS sq_ft, 
all_sites.lat AS lat, 
all_sites.lng AS lng,
all_sites.industry AS industry
ORDER BY date, site_id;

-- ########### fillin time: 69:51s / lines: 365 * 100 / stage: 5 

CREATE VIEW view_<description>(
 field1,
 field2,
 field3,
 ...
) AS SELECT (<your query>)
-- # to get the solution we simply perform a SELECT * FROM <my_view>









-- query 5
select industry,SUM(value/sq_ft) as ratio FROM work_table_day_axis group by industry order by ratio;

select industry,SUM(value/sq_ft) as ratio, season(date) as season FROM work_table_day_axis group by (industry,season);




CREATE TABLE panda.tab_orcbysite(
  timestamp bigint,
  dttm_utc string,
  value float,
  estimated bigint,
  anomaly string,
)
COMMENT 'Testing table for the first LD Curve on ORCFiles'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC;

# We fill the new table with the temporary table all_records
INSERT OVERWRITE TABLE rcforsum SELECT * FROM all_records;

#### SOLUTION 1 ####
SELECT dttm_utc, SUM(value) AS power FROM all_records GROUP BY dttm_utc ORDER BY dttm_utc;
# this quey return the sum for all sites per datetime
# dttm_utc, power
# 2012-01-01 00:05:00,25.4178009033
####################

# We create the partitioned table by industry

# We fill the new table with the temporary table all_records



# In order to distribute each records in the partitioned table by industry
# we join all_sites and all_recods to get the industry of the site for each records

## EXEMPLE: JOIN on the file name##
SELECT DISTINCT filename(all_records.INPUT__FILE__NAME), all_sites.industry 
FROM all_records JOIN all_sites 
ON filename(all_records.INPUT__FILE__NAME) = all_sites.site_id LIMIT 10;
################################### 











### QUESTION 7 ###
SELECT all_sites.site_id, all_sites.industry, all_sites.sub_industry, MAX(tab.conso) AS consumption FROM (

  SELECT filename(all_records.INPUT__FILE__NAME) AS site,
  getday(all_records.dttm_utc) AS date,
  SUM(all_records.value) AS conso FROM all_records
  GROUP BY all_records.INPUT__FILE__NAME, getday(all_records.dttm_utc)
  
) AS tab JOIN all_sites ON tab.site = all_sites.site_id 

GROUP BY all_sites.site_id, all_sites.industry, all_sites.sub_industry ORDER BY consumption;

### QUESTION 7 : Optimisation ### 
### No useless Join
SELECT tab.site, MAX(tab.conso) AS consumption FROM (

  SELECT filename(all_records.INPUT__FILE__NAME) AS site,
  getday(all_records.dttm_utc) AS date,
  SUM(all_records.value) AS conso FROM all_records
  GROUP BY all_records.INPUT__FILE__NAME, getday(all_records.dttm_utc)

  ) AS tab 
GROUP BY tab.site ORDER BY consumption;


-- # Util fonctions
DROP TEMPORARY FUNCTION [function_name];
-- DELETE JAR Bamboo-0.0.1-SNAPSHOT.jar;
DESCRIBE EXTENDED work_table_records;
DESCRIBE FORMATTED work_table_records;

--DROP TEMPORARY FUNCTION getweek;
--DROP TEMPORARY FUNCTION getday;
--DROP TEMPORARY FUNCTION filename;
--DROP TEMPORARY FUNCTION season;
--DROP TEMPORARY FUNCTION sumover;
--DROP TEMPORARY FUNCTION avgover;

-- ### IMPORT JARS
-- % filename
-- % org.isep.pandas.udf.FileName

-- % getweek
-- % org.isep.pandas.udf.Week

-- % getday
-- % org.isep.pandas.udf.Day

-- % seaon
-- % org.isep.pandas.udf.Season

-- % arraysum
-- % org.isep.pandas.udf.ArraySum

-- % arrayavg
-- % org.isep.pandas.udf.ArrayAvg

-- % sumover
-- % org.isep.pandas.udaf.SumOver