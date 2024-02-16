#!/bin/bash

ZONE_SZ=1024 #Mega Byte
BLOCK_SZ=4096 #Byte
NR_ZONE=50

SIZE=$(($ZONE_SZ*$NR_ZONE))

modprobe null_blk
cd /sys/kernel/config/nullb &&
    mkdir -p zns_nullb &&
    cd zns_nullb ; echo 0 > power; 
    echo 1 > zoned &&
    echo $ZONE_SZ > zone_size &&
    echo 0 > zone_nr_conv &&
    echo 0 > completion_nsec &&
    echo $BLOCK_SZ > blocksize &&
    echo $SIZE > size &&
    echo 1 > memory_backed &&
    echo 1 > power;
