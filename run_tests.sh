#!/bin/bash

ISTART=$1
IEND=$2
INCREM=$3
REPS=$4

for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run ${REPS} tile_raster /mnt/extdisk/data/merged_tiles/victorias_merge_$i.tif /mnt/extdisk/dump/victorias_merge_$i\" &> logs/tiling/victorias_$i_run_${REPS}.log"
   echo $CMD_STR
done
