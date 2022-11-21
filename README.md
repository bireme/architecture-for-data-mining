# ISIS2MongoDB project

## App versions
* Scala 3
* JDK 11
* MongoDB 6.0.2
* Mongo-scala-driver 4.7.2
* Docker 20.10

## CLI Usage

1. Set env variable `ISO_PATH` with the full path for the ISIS ISO file.
1. Set env variable `ISIS2MONGO_SETTINGS` with `cli`.
1. Run `sbt -Dfile.encoding=ISO-8859-1 run` (Enforce ISO-8859-1 encoding for proper ISIS database character manipulation)

## Docker Usage

1. Set env variable `ISO_PATH` with the full path for the ISIS ISO file.
1. `docker compose up` will run a service with MongoDB and another with ISIS2MongoDB that will take as input the env variable defined in the step above.

## Manipulating ISIS databases

This project leverages the [CISIS tool](https://red.bvsalud.org/en/wwwisis/) mainly through `MX` util.
In order to properly read each field, this whole project must be executed in Latin-1 (ISO-8859-1) encoding.

## MongoDB integration

Ideally, this project would leverage [Apache Spark](https://spark.apache.org/) but due to a lack of compatibility between Scala 3 and the Apache Spark plugin, it has been decided to leverage the [MongoDB Scala driver](http://mongodb.github.io/mongo-java-driver/4.7/driver-scala/)