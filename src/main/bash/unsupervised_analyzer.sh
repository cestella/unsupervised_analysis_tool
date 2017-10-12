#!/bin/bash

export VERSION=${project.version}
export NAME=${project.artifactId}
export JAR=$NAME-$VERSION-shaded.jar
spark-submit --class com.caseystella.analysis.CLI\
             --master yarn-client\
             --driver-memory 2g \
             --conf "spark.driver.maxResultSize=2048" \
             --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:+UseParNewGC" \
             --conf "spark.ui.showConsoleProgress=true" \
             ./$JAR \
             "$@"
