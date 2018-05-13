#!/bin/bash

ISTART=$1
IEND=$2
INCREM=$3
REPS=$4
SFC_INDEX=$5

### TILE RASTER ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run tile_raster ${REPS} /mnt/extdisk/data/merged_tiles/victorias_merge_$i.tif /mnt/extdisk/dump/tiles/victorias_merge_$i ${SFC_INDEX}\" &> logs/1_tiling/victorias_${i}_${SFC_INDEX}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done

### MAP META ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
    CMD_STR="./sbt \"run map_meta ${REPS} 000000${i} /mnt/extdisk/dump/tiles/victorias_merge_$i /home/spark/coverage/mindoro_coverage.shp\" &> logs/2_map_meta/victorias_${i}_${SFC_INDEX}_run_${REPS}.log"
    if [ $i -lt 10 ]
    then
        CMD_STR="./sbt \"run map_meta ${REPS} 0000000${i} /mnt/extdisk/dump/tiles/victorias_merge_$i /home/spark/coverage/mindoro_coverage.shp\" &> logs/2_map_meta/victorias_${i}_${SFC_INDEX}_run_${REPS}.log"
    fi

    echo "Running: $CMD_STR"
    eval $CMD_STR
done

### INVERTED INDEX ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run inverted_idx ${REPS} /mnt/extdisk/dump/tiles/victorias_merge_$i/json /home/spark/coverage/mindoro_coverage.shp\" &> logs/3_invt_idx/victorias_${i}_${SFC_INDEX}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done

### QUERY ###
for (( i=${ISTART}; i<=${IEND}; i+=${INCREM}))
do
   CMD_STR="./sbt \"run query_shp ${REPS} /mnt/extdisk/dump/tiles/victorias_merge_$i /home/spark/coverage/mindoro_victoria/victoria.shp /mnt/extdisk/dump/queries/victorias_${i}.tif ${SFC_INDEX}\" &> logs/4_queries/victorias_${i}_${SFC_INDEX}_run_${REPS}.log"
   echo "Running: $CMD_STR"
   eval $CMD_STR
done
