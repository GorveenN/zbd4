#!/bin/bash

declare -a PROCS=( 0.001 0.003 0.006 0.009 )

for p in {1..32}; do
    for i in "${PROCS[@]}"; do
        echo $i
        echo $p
        python $1 --num-proc $p --num-iter 1000 --results $2 --sleep $i
    done
done
