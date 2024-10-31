#!/bin/bash

function handler() {
    kill -s SIGTERM $PID
}
trap handler SIGTERM

addr=$(host $(hostname) | awk '{print $NF}')
port=2020


#   -e xpptut15 -r 580 -d jungfrau4M -m image -a tcp://$addr:$port -c smd --img_per_file 20
#ssh psana sbatch /sdf/home/r/rogersdd/venvs/run_psana_push \

/sdf/home/r/rogersdd/venvs/psana_cached_push $((port-1)) $port \
    -e xpptut15 -r 671 -c smd -d epix10k2M -m calib -n 1 &

PID=$!
echo "Started producer $PID"
sleep 0.1

trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

#kill $PID
psana_pull -d tcp://$addr:$port

