#!/bin/bash
# Sends a file from psana-accessible project data.

if [ $# -ne 2 ]; then
    echo "Usage: $0 <file name> <src port>"
    exit 1
fi
fname=$1
port=$2

addr=$(host $(hostname) | awk '{print $NF}')

echo "To receive, run: file_pull tcp://$addr:$port <outname>"
/sdf/home/r/rogersdd/venvs/file_cached_push $((port-1)) $port "$fname"
