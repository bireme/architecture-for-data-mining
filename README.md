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

## CLI Usage

1. Set env variable `ISIS2MONGO_SETTINGS` with `cli`.
1. Edit the `src/main/resources/cli.conf` config file.
1. Run `sbt -Dfile.encoding=ISO-8859-1 run` (Enforce ISO-8859-1 encoding for proper ISIS database character manipulation)

## Docker Usage

1. Edit the `src/main/resources/docker.conf` config file.
1. `docker compose up` will run a service with MongoDB and another with ISIS2MongoDB that will take as input the env variable defined in the step above.

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