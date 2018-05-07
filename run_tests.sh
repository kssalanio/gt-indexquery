#!/bin/bash

ISTART=$1
IEND=$2
INCREM=$3
REPS=$4

### TILE RASTER ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run tile_raster ${REPS} /mnt/extdisk/data/merged_tiles/victorias_merge_$i.tif /mnt/extdisk/dump/tiles/victorias_merge_$i\" &> logs/1_tiling/victorias_${i}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done

### MAP META ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run map_meta ${REPS} /mnt/extdisk/dump/tiles/victorias_merge_$i /home/spark/coverage/mindoro_coverage.shp\" &> logs/2_map_meta/victorias_${i}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done

### INVERTED INDEX ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run inverted_idx ${REPS} /mnt/extdisk/dump/tiles/victorias_merge_$i/json /home/spark/coverage/mindoro_coverage.shp\" &> logs/3_invt_idx/victorias_${i}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done

### QUERY ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run query_shp ${REPS} /mnt/extdisk/dump/tiles/victorias_merge_$i /home/spark/coverage/mindoro_victoria/victoria.shp /mnt/extdisk/dump/queries/victorias_25.tif\" &> logs/4_queries/victorias_${i}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done

