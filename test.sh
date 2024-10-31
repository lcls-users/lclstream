#!/bin/bash

function handler() {
    kill -s SIGTERM $PID
}
trap handler SIGTERM

addr=$(host $(hostname) | awk '{print $NF}')
port=2020
python3 lclstream/psana_pull.py -l tcp://$addr:$port &
PID=$!
echo "Started puller pid $PID"


#   -e xpptut15 -r 580 -d jungfrau4M -m image -a tcp://$addr:$port -c smd --img_per_file 20
ssh psana sbatch /sdf/home/r/rogersdd/venvs/run_psana_push \
    -e xpptut15 -r 671 -c smd -d epix10k2M -m calib -n 1 -a tcp://$addr:$port

#kill $PID
