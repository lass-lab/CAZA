#!/bin/bash
rm reset_file.txt df_file.txt lsm_state.txt compactions.txt

sudo rmdir /sys/kernel/config/nullb/zns_nullb

sudo ./setup_zone_nullblk.sh

DEV=nullb1

# We need the deadline io scheduler to gurantee write ordering
echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=nullb1 --aux_path=/mnt/db --finish_threshold=5 --force

./db_bench \
     --fs_uri=zenfs://dev:$DEV \
     --benchmarks=fillrandom,overwrite,stats \
     -statistics \
     -db=./db \
     --num=150000000 \
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
     -target_file_size_multiplier=1 \
     -verify_checksum=true \
