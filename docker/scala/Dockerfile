FROM adoptopenjdk/openjdk11:alpine
RUN apk --no-cache add curl bash

RUN apk add --no-cache --update musl musl-utils musl-locales
ENV LANG en_US.ISO-8859-1
ENV LANGUAGE en_US.ISO-8859-1
ENV LC_ALL en_US.ISO-8859-1

ENV SBT_VERSION 1.7.1
ENV ISIS2MONGO_SETTINGS docker

RUN curl -L -o sbt-$SBT_VERSION.zip https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.zip
RUN unzip sbt-$SBT_VERSION.zip -d opt

WORKDIR /isis-to-mongodb
ADD . /isis-to-mongodb

CMD /opt/sbt/bin/sbt -J-Xmx4G -J-XX:+UseG1GC -Dfile.encoding=ISO-8859-1 run
