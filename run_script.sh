#!/bin/bash

DEV=nullb1
ZONE_CAP_SECS=$(./blkzone report -c 5 /dev/$DEV | grep -oP '(?<=len )[0-9xa-f]+' | head -1)
FUZZ=5
ZONE_CAP=$((ZONE_CAP_SECS * 512))
BASE_FZ=$(($ZONE_CAP  * (100 - $FUZZ) / 100))
WB_SIZE=$(($BASE_FZ * 2))

TARGET_FZ_BASE=$WB_SIZE
TARGET_FILE_SIZE_MULTIPLIER=2
MAX_BYTES_FOR_LEVEL_BASE=$((2 * $TARGET_FZ_BASE))

echo "ZONE_CAP_SECS"
echo $ZONE_CAP_SECS

echo "ZONE_CAP"
echo $ZONE_CAP

echo "BASE_FZ"
echo $BASE_FZ

echo "WB_SIZE"
echo $WB_SIZE

echo "TARGET_FZ_BASE"
echo $TARGET_FZ_BASE

echo "MAX_BYTES_FOR_LEVEL_BASE"
echo $MAX_BYTES_FOR_LEVEL_BASE

# We need the deadline io scheduler to gurantee write ordering
echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=$DEV --aux_path=/tmp/zenfs_$DEV --finish_threshold=$FUZZ --force

#origin
#./db_bench --fs_uri=zenfs://dev:$DEV --key_size=16 --value_size=4096 --target_file_size_base=$TARGET_FZ_BASE --write_buffer_size=$WB_SIZE --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE --max_bytes_for_level_multiplier=4 --use_direct_io_for_flush_and_compaction --max_background_jobs=$(nproc) --num=1000000 --benchmarks="fillrandom"

# 80G
 ./db_bench --fs_uri=zenfs://dev:$DEV --key_size=16 --value_size=4096 --num=20889920 --use_direct_io_for_flush_and_compaction --max_background_jobs=$(nproc) --benchmarks="fillrandom" --compression_ratio=1
