#!/bin/sh
set -eu
mkdir -p /workspace/artifacts/graphx-classes
java -cp '/opt/spark/jars/*' scala.tools.nsc.Main -usejavacp -d /workspace/artifacts/graphx-classes /workspace/src/GraphAnalysis.scala
python3 /workspace/tools/package_graphx.py
spark-submit --class GraphAnalysis --master spark://spark-master:7077 --driver-memory 768m /workspace/artifacts/graphx.jar
