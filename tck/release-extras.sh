#!/bin/sh
sbt publishM2

rsync -Pavuz $HOME/.m2/repository/org/reactivestreams/* \
      akkarepo@repo.akka.io:/home/akkarepo/www/snapshots/org/reactivestreams
