#!/bin/bash

#Write 80GB KV
#TOTAL 100GB DEVICE
ZONE_SZ=256
NR_ZONE=300
SIZE=$((${ZONE_SZ}*${NR_ZONE}))

DEV=0
ZONE_CAP_SECS=0
FUZZ=0
echo $SIZE

while [ $ZONE_SZ -le 256 ]
do

modprobe null_blk
cd /sys/kernel/config/nullb &&
    mkdir -p zns_nullb &&
    cd zns_nullb ; echo 0 > power;
    echo 1 > zoned &&
    echo $ZONE_SZ > zone_size &&
    echo 0 > zone_nr_conv &&
    echo 0 > completion_nsec &&
    echo 4096 > blocksize &&
    echo $SIZE > size &&
    echo 1 > memory_backed &&
    echo 1 > power;

cd ~/JohnFS
DEV=nullb1
ZONE_CAP_SECS=$(./blkzone report -c 5 /dev/$DEV | grep -oP '(?<=len )[0-9xa-f]+' | head -1)
FUZZ=5
ZONE_CAP=$((ZONE_CAP_SECS * 512))
BASE_FZ=$(($ZONE_CAP  * (100 - $FUZZ) / 100))
WB_SIZE=$(($BASE_FZ * 2))

TARGET_FZ_BASE=$WB_SIZE
TARGET_FILE_SIZE_MULTIPLIER=2
MAX_BYTES_FOR_LEVEL_BASE=$((2 * $TARGET_FZ_BASE))

# We need the deadline io scheduler to gurantee write ordering
echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=$DEV --aux_path=/tmp/zenfs_$DEV --finish_threshold=$FUZZ --force

# 80GB RAW SIZE
./db_bench --fs_uri=zenfs://dev:$DEV --key_size=16 --value_size=4096 --num=20889920 --use_direct_io_for_flush_and_compaction --max_background_compactions=1 -max_background_flushes=1 --benchmarks="fillrandom" --compression_ratio=1 --disable_wal=true > ~/EXP_RES/RAW/${ZONE_SZ}.txt

mv compactions.txt ~/EXP_RES/RAW/${ZONE_SZ}_CompactionTable.txt
mv lsm_state.txt ~/EXP_RES/RAW/${ZONE_SZ}_LSMHistoryTable.txt
#python3 ./scripts/zone_info_parser.py ~/EXP_RES/RAW/${ZONE_SZ}.txt ~/EXP_RES/RAW/${ZONE_SZ}_CompactionTable.txt ~/EXP_RES/CSV/${ZONE_SZ}.csv

rmdir /sys/kernel/config/nullb/zns_nullb

ZONE_SZ=$((${ZONE_SZ}*2))
NR_ZONE=$((${NR_ZONE}/2))
SIZE=$((${ZONE_SZ}*${NR_ZONE}))

done
