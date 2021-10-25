#!/bin/bash

DEV=nullb1

# We need the deadline io scheduler to gurantee write ordering
#echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=nullb1 --aux_path=/mnt/db --finish_threshold=5 --force

./db_bench \
     --fs_uri=zenfs://dev:$DEV \
     --benchmarks=fillrandom,overwrite,stats \
     -statistics \
     -db=./db \
     --num=110000000 \
     -write_buffer_size=67108864 \
     --threads=4 \
     -disable_wal=true \
     -report_interval_seconds=1 \
     -stats_dump_period_sec=5 \
     --key_size=16 \
     --value_size=128 \
     -max_background_compactions=10 \
     -max_background_flushes=10 \
     -compression_ratio=1 \
     -use_direct_io_for_flush_and_compaction \
     -verify_checksum=false \

     
#--threads=4 \
#-verify_checksum=false \
#--num=157797872 \
#--db=./db 
#-compression_type=none \


#cp /mnt/db/LOG ~/fillrandom.log
#sleep 30

#./db_bench \
#     --fs_uri=zenfs://dev:$DEV \
#     --benchmarks=overwrite,stats \
#     -statistics \
#     --num=131151482 \
#     --threads=1 \
#     --db=./db \
#     -disable_wal=true \
#     -write_buffer_size=67108864 \
#     -report_interval_seconds=1 \
#     -stats_dump_period_sec=5 \
#     --key_size=16 \
#     --value_size=128 \
#     --use_existing_db=1 \
#     --use_existing_keys=1 \
#     -max_background_compactions=10 \
#     -max_background_flushes=10 \
#     -compression_type=none \
#     -verify_checksum=false \
#     -compression_ratio=1
