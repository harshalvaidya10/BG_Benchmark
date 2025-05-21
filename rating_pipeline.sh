#!/bin/bash

# 默认参数（可通过命令行传入覆盖）
WORKLOAD="workloads/ReadOnlyActions"
POPULATE_WORKLOAD="workloads/populateDB_100000"
LATENCY="0.1"
PERC="99"
STALENESS="0.01"
DURATION="60"
DIRECTORY="exp_soar_cache_1000"
MINIMUM="40"
OBJECTIVE="soar"
VALIDATION="false"
DOCACHE="true"
DOLOAD="false"
DOMONITOR="false"
DOWARMUP="false"

while [[ $# -gt 0 ]]; do
  case $1 in
    -workload) WORKLOAD="$2"; shift ;;
    -populateWorkload) POPULATE_WORKLOAD="$2"; shift ;;
    -latency) LATENCY="$2"; shift ;;
    -perc) PERC="$2"; shift ;;
    -staleness) STALENESS="$2"; shift ;;
    -duration) DURATION="$2"; shift ;;
    -directory) DIRECTORY="$2"; shift ;;
    -minimum) MINIMUM="$2"; shift ;;
    -objective) OBJECTIVE="$2"; shift ;;
    -validation) VALIDATION="$2"; shift ;;
    -doCache) DOCACHE="$2"; shift ;;
    -doLoad) DOLOAD="$2"; shift ;;
    -doMonitor) DOMONITOR="$2"; shift ;;
    -doWarmup) DOWARMUP="$2"; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

# Step 1
echo "Running Step 1: JanusGraphBGCoord..."
java -Xmx14000m -XX:ActiveProcessorCount=14 -Dlogback.configurationFile=/data/bg/conf/logback.xml -cp "target/BG-1.0-SNAPSHOT.jar:target/lib/*" edu.usc.bg.JanusGraphBGCoord \
  -workload "$WORKLOAD" \
  -doLoad "$DOLOAD" \
  -doMonitor "$DOMONITOR" \
  -doCache "$DOCACHE" \
  -doWarmup "$DOWARMUP" \
  -populateWorkload "$POPULATE_WORKLOAD" \
  -latency "$LATENCY" \
  -perc "$PERC" \
  -staleness "$STALENESS" \
  -duration "$DURATION" \
  -directory "$DIRECTORY" \
  -minimum "$MINIMUM" \
  -objective "$OBJECTIVE" \
  -validation "$VALIDATION"

# Step 2
#echo "Running Step 2: LogRetryStatsFromDir..."
#java -cp "target/classes:target/lib/*" scripts.LogRetryStatsFromDir "$DIRECTORY"

# Step 3
echo "Running Step 3: extract_rating_statistics.sh..."
# 传入 directory 参数给 extract_rating_statistics.sh
./extract_rating_statistics.sh "$DIRECTORY"
