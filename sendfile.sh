#!/bin/bash
# Sends a file from psana-accessible project data.

if [ $# -ne 2 ]; then
    echo "Usage: $0 <file name> <src port>"
    fname=$1
    port=$2
fi

addr=$(host $(hostname) | awk '{print $NF}')

/sdf/home/r/rogersdd/venvs/file_cached_push $((port-1)) $port "$fname"

echo "To receive, run: file_pull tcp://$addr:$port <outname>"
