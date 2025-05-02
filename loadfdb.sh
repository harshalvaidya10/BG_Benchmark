#!/bin/bash
#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <host1> [<host2> ...]"
  exit 1
fi

user="Ziqif"
# 传入的参数即完整的 host 名称或 IP
hosts=("$@")

for fqdn in "${hosts[@]}"; do
  echo "------ Processing ${user}@${fqdn} ------"

  ssh "${user}@${fqdn}" bash -l -c "'
    LOGFILE=/var/log/fdb_copy_time.log
    sudo touch \"\$LOGFILE\" && sudo chown $user:\"\$LOGFILE\"

    echo \"\n=== \$(date '+%Y-%m-%d %H:%M:%S') on \$(hostname) ===\" | tee -a \"\$LOGFILE\"

    echo \"Stopping FoundationDB service...\" | tee -a \"\$LOGFILE\"
    sudo service foundationdb stop

    # 备份数据
    echo \"\$(date '+%Y-%m-%d %H:%M:%S') START backup copy\" | tee -a \"\$LOGFILE\"
    t_start=\$(date +%s)
    sudo cp -r /var/lib/foundationdb/data/ /var/lib/foundationdb/copy/
    t_end=\$(date +%s)
    echo \"\$(date '+%Y-%m-%d %H:%M:%S') END   backup copy, duration: \$((t_end-t_start))s\" | tee -a \"\$LOGFILE\"

    echo \"Starting FoundationDB service...\" | tee -a \"\$LOGFILE\"
    sudo service foundationdb start
    sleep 5

    echo \"Stopping FoundationDB service for restore...\" | tee -a \"\$LOGFILE\"
    sudo service foundationdb stop

    echo \"Removing old data directory...\" | tee -a \"\$LOGFILE\"
    sudo rm -rf /var/lib/foundationdb/data/

    # 恢复数据
    echo \"\$(date '+%Y-%m-%d %H:%M:%S') START restore copy\" | tee -a \"\$LOGFILE\"
    t_start=\$(date +%s)
    sudo cp -r /var/lib/foundationdb/copy/ /var/lib/foundationdb/data/
    t_end=\$(date +%s)
    echo \"\$(date '+%Y-%m-%d %H:%M:%S') END   restore copy, duration: \$((t_end-t_start))s\" | tee -a \"\$LOGFILE\"

    echo \"Starting FoundationDB service after restore...\" | tee -a \"\$LOGFILE\"
    sudo service foundationdb start

    echo \"Done on \$(hostname)!\" | tee -a \"\$LOGFILE\"
  '"

  echo
done

echo "All hosts processed."
