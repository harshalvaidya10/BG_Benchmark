#!/bin/bash

# 默认参数
WORKLOAD="workloads/ReadOnlyActions"
POPULATE_WORKLOAD="workloads/populateDB_1000"
THREADCOUNT="100"
DURATION="180"
DIRECTORY="cacheLog"
VALIDATION="false"
# 输出文件名将存放在 DIRECTORY 中
LOADINGWORKLOADOUTPUT="loadingWorkloadOutput.log"
WORKLOADOUTPUT="workloadOutput.log"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
  case $1 in
    -workload) WORKLOAD="$2"; shift ;;
    -populateWorkload) POPULATE_WORKLOAD="$2"; shift ;;
    -thread) THREADCOUNT="$2"; shift ;;
    -duration) DURATION="$2"; shift ;;
    -directory) DIRECTORY="$2"; shift ;;
    -validation) VALIDATION="$2"; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

# 设置输出文件路径，确保目录存在
mkdir -p "$DIRECTORY"
WORKLOADOUTPUT="$DIRECTORY/workloadOutput.log"
LOADINGWORKLOADOUTPUT="$DIRECTORY/loadingWorkloadOutput.log"

# 可选：如果没有定义 LOG_FILE，则用下面的默认名称
LOG_FILE="${LOG_FILE:-script.log}"

# Step 1: 清除旧日志及文件
echo "Removing old read and update files..." | tee -a "$LOG_FILE"
sudo rm -f read*.txt
sudo rm -f update*.txt

#echo "Starting database population..." | tee -a "$LOG_FILE"
#
#echo "Clearing JanusGraph database before running this combination..." | tee -a "$LOG_FILE"
#"$GREMLIN_CONSOLE" <<EOF
#:remote connect tinkerpop.server $REMOTE_CONFIG
#:remote console
#:remote config timeout 600000
#g.E().drop()
#g.V().drop()
#:exit
#EOF
#
## 运行 populate workload
#echo "Starting database population workload..." | tee -a "$LOADINGWORKLOADOUTPUT"
#java -cp "build/classes:lib/*" edu.usc.bg.BGMainClass onetime -load edu.usc.bg.workloads.UserWorkLoad -threads 10 -db janusgraph.JanusGraphClient -P "$POPULATE_WORKLOAD" 2>&1 | tee "$LOADINGWORKLOADOUTPUT" &
#PID1=$!
#
## 监测 LOADINGWORKLOADOUTPUT 中的 "SHUTDOWN" 关键字
#while sleep 2; do
#    if grep -q "SHUTDOWN" "$LOADINGWORKLOADOUTPUT"; then
#        echo "Detected SHUTDOWN - Waiting for process $PID1 to exit..."
#        kill -9 "$PID1"
#        echo "Database population complete." | tee -a "$LOADINGWORKLOADOUTPUT"
#        break
#    fi
#done

# 运行主工作负载
echo "Starting main workload..." | tee -a "$WORKLOADOUTPUT"
java -cp "build/classes:lib/*" edu.usc.bg.BGMainClass onetime -t edu.usc.bg.workloads.CoreWorkLoad -threads "$THREADCOUNT" -maxexecutiontime "$DURATION" -db janusgraph.JanusGraphClient -P "$WORKLOAD" -s true 2>&1 | tee "$WORKLOADOUTPUT" &
PID2=$!

# 监测 WORKLOADOUTPUT 中的结束关键字
while sleep 2; do
    if grep -q "Stop requested for workload. Now Joining!" "$WORKLOADOUTPUT"; then
        echo "Detected workload shutdown - Waiting for process $PID2 to exit..."
        kill -9 "$PID2"
        echo "Workload complete." | tee -a "$WORKLOADOUTPUT"
        break
    fi
done
