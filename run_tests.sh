#!/bin/bash

ISTART=$1
IEND=$2
INCREM=$3
REPS=$4

for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run tile_raster ${REPS} /mnt/extdisk/data/merged_tiles/victorias_merge_$i.tif /mnt/extdisk/dump/victorias_merge_$i\" &> logs/1_tiling/victorias_$i_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval CMD_STR
done

for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run map_meta ${REPS} /mnt/extdisk/dump/victorias_merge_$i /home/spark/coverage/mindoro_coverage.shp\" &> logs/2_map_meta/victorias_$i_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval CMD_STR
done
