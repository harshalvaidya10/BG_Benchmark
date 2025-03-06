#!/bin/bash

GREMLIN_HOME="$HOME/janusgraph-1.1.0"
GREMLIN_CONSOLE="$GREMLIN_HOME/bin/gremlin.sh"
REMOTE_CONFIG="$GREMLIN_HOME/conf/remote.yaml"

LOG_FILE="validation_results_1.log"
TMP_OUTPUT1="tmp_output1.log"
TMP_OUTPUT2="tmp_output2.log"
TMP_OUTPUT3="tmp_output3.log"

> "$LOG_FILE"

PopulateAction="populateDB_1000"
ValidationAction="SymmetricMixDelegateAction"

threads_variants=4
threadcount=4
validationthreads=15

echo "Removing old read and update files..." | tee -a "$LOG_FILE"
sudo rm -f read*.txt
sudo rm -f update*.txt

echo "Starting database population..." | tee -a "$LOG_FILE"

echo "Clearing JanusGraph database before running this combination..." | tee -a "$LOG_FILE"
"$GREMLIN_CONSOLE" <<EOF
:remote connect tinkerpop.server $REMOTE_CONFIG
:remote console
:remote config timeout 600000
g.E().drop()
g.V().drop()
:exit
EOF

java -cp "build/classes:lib/*" edu.usc.bg.BGMainClass onetime -load edu.usc.bg.workloads.UserWorkLoad -threads 1 -db janusgraph.JanusGraphClient -P "workloads/$PopulateAction" 2>&1 | tee "$TMP_OUTPUT1" &
PID1=$!

# 监测 SHUTDOWN 关键字
while sleep 2; do
    if grep -q "SHUTDOWN" "$TMP_OUTPUT1"; then
        echo "Detected SHUTDOWN - Waiting for process $PID1 to exit..."
        kill -9 "$PID1"
        echo "Database population complete." | tee -a "$LOG_FILE"
        break
    fi
done

operationcount_file="workloads/$ValidationAction"
if [ ! -f "$operationcount_file" ]; then
    echo "Error: $operationcount_file not found!" | tee -a "$LOG_FILE"
fi

expected_actions=$(grep "^operationcount=" "$operationcount_file" | cut -d '=' -f2)
java -cp "build/classes:lib/*" edu.usc.bg.BGMainClass onetime -t edu.usc.bg.workloads.CoreWorkLoad -threads "$threads_variants" -db janusgraph.JanusGraphClient -P "workloads/$ValidationAction" -s true 2>&1 | tee $TMP_OUTPUT2 &
PID2=$!

# 运行 10 分钟后终止
while sleep 2; do
    ACTIONS_LINE=$(grep -o "[0-9]\+ sec: [0-9]\+ actions; .*" $TMP_OUTPUT2 | tail -n 1)
    if [[ -n "$ACTIONS_LINE" ]]; then
        actual_actions=$(echo "$ACTIONS_LINE" | awk '{print $3}')
        if [[ "$actual_actions" -eq "$expected_actions" ]]; then
            echo "Detected '$ACTIONS_LINE' with expected actions $expected_actions - Killing process $PID2"
            kill -9 "$PID2"

            # 记录日志
            echo "Result: $ACTIONS_LINE" | tee -a "$LOG_FILE"
            break
        fi
    fi
done


echo "Starting validation with $threads_variants threads..." | tee -a "$LOG_FILE"

java -cp "build/classes:lib/*" edu.usc.bg.validator.ValidationMainClass -t edu.usc.bg.workloads.CoreWorkLoad -threadcount "$threadcount" -validationthreads "$validationthreads" -db janusgraph.JanusGraphClient -P workloads/SymmetricMixDelegateAction -threads "$threads_variants" 2>&1 | tee "$TMP_OUTPUT3" &
PID3=$!

echo "Started validation process with PID: $PID3"
echo "Extracting validation results for $threads_variants threads..." | tee -a "$LOG_FILE"

# 监听日志文件，一旦匹配到 "X% of reads observed the value of ..." 立即执行操作
MATCHED_LINE=$(grep -m 1 -E '[0-9]+% of reads observed the value of ' <(tail -f "$TMP_OUTPUT3"))

# 新增日志文件用于记录 "Data was stale..."
STALE_LOG_FILE="stale_data.log"

# 监听 TMP_OUTPUT3，匹配 "X% of reads observed the value of ..." 和 "Data was stale..."
tail -f "$TMP_OUTPUT3" | while read -r line; do
    # 监听 "X% of reads observed the value of ..."
        # 监听 "Data was stale..."
    if [[ "$line" == *"Data was stale..."* ]]; then
        echo "$line" | tee -a "$STALE_LOG_FILE"  # 记录到 stale_data.log
    fi
    if [[ "$line" =~ [0-9]+%[[:space:]]of[[:space:]]reads[[:space:]]observed[[:space:]]the[[:space:]]value[[:space:]]of ]]; then
        echo "$line" | tee -a "$LOG_FILE"  # 记录匹配行
        read -r next1 && echo "$next1" | tee -a "$LOG_FILE"  # 读取并记录下一行
        read -r next2 && echo "$next2" | tee -a "$LOG_FILE"  # 读取并记录再下一行
        read -r next3 && echo "$next3" | tee -a "$LOG_FILE"  # 读取并记录再下一行
        read -r next4 && echo "$next4" | tee -a "$LOG_FILE"  # 读取并记录再下一行
        echo "Validation result detected, killing process $PID3..." | tee -a "$LOG_FILE"
        kill -9 "$PID3"
        break
    fi
done


echo "Validation with $threads_variants threads completed. Results logged." | tee -a "$LOG_FILE"
echo "All validations completed. Results saved in $LOG_FILE."







