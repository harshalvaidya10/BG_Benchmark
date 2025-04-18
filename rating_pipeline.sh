#!/bin/bash

# 默认参数（可通过命令行传入覆盖）
WORKLOAD="workloads/ReadOnlyActions"
POPULATE_WORKLOAD="workloads/populateDB_1000"
LATENCY="0.1"
PERC="99"
STALENESS="0.01"
DURATION="60"
DIRECTORY="exp_soar_cache_1000"
MINIMUM="5"
OBJECTIVE="soar"
VALIDATION="false"
DOCACHE="true"
DOLOAD="false"
DOMONITOR="false"

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
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

# PreLoad
if [[ "${DOLOAD,,}" == "false" ]]; then
  echo "==> Pre‑loading database with $POPULATE_WORKLOAD …"
  TMP_OUTPUT1="$(mktemp)"
  LOG_FILE="populate_$(date +%Y%m%d_%H%M%S).log"

  java -cp "build/classes:lib/*" \
       edu.usc.bg.BGMainClass onetime -load \
       edu.usc.bg.workloads.UserWorkLoad \
       -threads 10 -db janusgraph.JanusGraphClient \
       -P "$POPULATE_WORKLOAD" 2>&1 | tee "$TMP_OUTPUT1" &
  PID1=$!

  # 轮询日志，检测 "SHUTDOWN" 关键字
  while sleep 2; do
    if grep -q "SHUTDOWN" "$TMP_OUTPUT1"; then
      echo "Detected SHUTDOWN — waiting for process $PID1 to exit …"
      # 尝试优雅终止，5 秒后仍在则强杀
      kill -TERM "$PID1" 2>/dev/null || true
      for i in {1..5}; do
        sleep 1
        if ! kill -0 "$PID1" 2>/dev/null; then break; fi
      done
      kill -9 "$PID1" 2>/dev/null || true
      echo "Database population complete." | tee -a "$LOG_FILE"
      break
    fi

    # 提前结束（进程意外退出）
    if ! kill -0 "$PID1" 2>/dev/null; then
      echo "Population process exited unexpectedly." | tee -a "$LOG_FILE"
      break
    fi
  done
fi


# Step 1
echo "Running Step 1: JanusGraphBGCoord..."
java -cp "build/classes:lib/*" edu.usc.bg.JanusGraphBGCoord \
  -workload "$WORKLOAD" \
  -doLoad "$DOLOAD" \
  -doMonitor "$DOMONITOR" \
  -doCache "$DOCACHE" \
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
echo "Running Step 2: LogRetryStatsFromDir..."
java -cp "build/classes:lib/*" scripts.LogRetryStatsFromDir "$DIRECTORY"

# Step 3
echo "Running Step 3: extract_rating_statistics.sh..."
# 传入 directory 参数给 extract_rating_statistics.sh
./extract_rating_statistics.sh "$DIRECTORY"
