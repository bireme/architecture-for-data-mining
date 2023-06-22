# ISIS2MongoDB project

## App versions
* Scala 3
* JDK 11
* MongoDB 6.0.2
* Mongo-scala-driver 4.7.2
* Mysql-connector-java 8.0.31
* Typesafe Config 1.4.2
* Scribe 3.10.4
* Docker 20.10

## Setting up batch-replace (Optional)
Create numbered CSV files in `./data/replace/` with the following format:
`FIELD,SUBFIELD,OLD VALUE,NEW VALUE`

Example:
`4,text,Web,NewWeb`
`8,_i,pt,pt-br`

This replace feature is applied to the `01_isiscopy` collection immediately after all records are migrated from ISIS to MongoDB.

A good convetion for CSV naming is the following:
`01_field_30.csv`
`02_field_08.csv`

This feature will list all files alphabetically from the `./data/replace/` folder and run each in order.

## Installing

1. Clone repository
1. Add your CSV files in the `data/replace/` folder (optional)
1. Place the Dedup indexes folders `lilacs_MNT`, `lilacs_MNTam` and `lilacs_Sas` in `data/Dedup/indexes`
1. Edit the MONGODB, ISO_PATH, INTEROPERABILITY_SOURCE, FIADMIN_MYSQL params in `src/main/resources/docker.conf` (for Docker) OR `src/main/resources/cli.conf` (for CLI) config file

### How to run in Docker

1. `docker-compose build` will compile the whole project.
1. `docker compose up` will run a service with MongoDB and another with ISIS2MongoDB.

### How to run in CLI

1. Run `sh run_cli.sh`

## Manipulating ISIS databases

This project leverages the [CISIS tool](https://red.bvsalud.org/en/wwwisis/) mainly through `MX` util.
In order to properly read each field, this whole project must be executed in Latin-1 (ISO-8859-1) encoding.

## MongoDB integration

Ideally, this project would leverage [Apache Spark](https://spark.apache.org/) but due to a lack of compatibility between Scala 3 and the Apache Spark plugin, it has been decided to leverage the [MongoDB Scala driver](http://mongodb.github.io/mongo-java-driver/4.7/driver-scala/)

## FI-Admin integration

FI-Admin's database runs in MySQL Server and is used for parts of the data transformation process. The JDBC connector available in `Mysql-connector-java` was leveraged in this part of the process.

This database is mainly used for field validation in the Transformation process. Invalid data is logged in a log file as described in `Logging (Scribe)`

## Logging (Scribe)

Scribe was leveraged as the project's logging feature. Every time this app is executed, a set of log files will be placed under the `logs` folder in the user's home folder.