#!/bin/bash
fdbcli --exec $'writemode on; clearrange \x00 \xff; exit'
