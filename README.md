# ISIS2MongoDB project

## App versions
* Scala 3
* JDK 11
* MongoDB 6.0.2
* Docker 20.10

## Usage CLI

1. Set env variable `ISO_PATH` with the full path for the ISIS ISO file.
1. Set env variable `ISIS2MONGO_SETTINGS` with `cli`.
1. Run `sbt -Dfile.encoding=ISO-8859-1 run` (Enforce ISO-8859-1 encoding for proper ISIS database character manipulation)

## Usage Docker

1. Set env variable `ISO_PATH` with the full path for the ISIS ISO file.
1. `docker compose up` will run a service with MongoDB and another with ISIS2MongoDB that will take as input the env variable defined in the step above.