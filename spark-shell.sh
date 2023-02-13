#!/bin/bash
docker run -it --name my-spark -p 4040:4040 -p 8080:8080 apache/spark /opt/spark/bin/spark-shell