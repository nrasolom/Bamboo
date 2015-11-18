# Bamboo
###### By The team Pandas
![Pandas Corp. Logo](https://dl.dropboxusercontent.com/s/e57l5e47rqtq39q/PandasCorp_githubbanner.png?dl=0)

This is a part of the ISEP'15 Bigdata project.
Bamboo is a set of functions to run Custom HIVE UDF

Prerequisites
-------------
You need the following tools to work on this project
* Java (at least 1.7)
* Maven
  * dependencies: Hadoop-client 2.7.0
  * dependencies: hive-exec 0.13.1
  * dependencies: junit 4.11
  * dependencies: joda-time 2.7
* Apache Hive

Installation
------------

1. fork it
2. maven install
3. maven build: package
4. load the `.jar` in hue

Tools: for this project the team use hue3 (See [hue](https://github.com/cloudera/hue/blob/master/README.md) repo)

Goal
------------
The main goal of the project is design queries able to retrieve the Load Curve.

>A Load Curve is a time series of electricity consumption. This project refer to it by "LD".


Usage
------------
```sql
SELECT datetime AS datetime, arraysum(values) AS total
FROM work_sitesbydate_array
ORDER BY datetime;
```
*Here the column "values" is an Array<float>, an Hive type. The function arraysum is a custom UDF from this projet and make the computation of the LD (LoaD curve) run faster.*

Design
------------
Here is an exemple of work table build for the queries. Tables are filled with the [EnerNOC](http://www.enernoc.com) open dataset. Find the measure over on year (2012) for one undred sites -----> [HERE](https://open-enernoc-data.s3.amazonaws.com/anon/index.html).

datetime: String	   | industry  |	values: Array\<float\>  
---------------------|-----------|--------------------------
2015-01-01 00:05:00  | Education |	\[, , , , , \] (x25)
2015-01-01 00:05:00  | Commerce  |	\[, , , , , \] (x25)
2015-01-01 00:05:00  | Food      |	\[, , , , , \] (x25)

The Team
------------
Pandas Corp. members are:
[@dchanthavong](https://github.com/dchanthavong),
[@RomainPhilippe](https://github.com/RomainPhilippe),
[@nrasolom](https://github.com/nrasolom),
[@DivLoic](https://github.com/DivLoic).
