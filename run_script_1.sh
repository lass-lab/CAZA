#!/bin/bash

DEV=nullb1

# We need the deadline io scheduler to gurantee write ordering
echo deadline > /sys/class/block/$DEV/queue/scheduler

./zenfs mkfs --zbd=nullb1 --aux_path=/mnt/db

./db_bench \
     --fs_uri=zenfs://dev:$DEV \
     --benchmarks=fillrandom,stats \
     -statistics \
     --num=110000000 \
     --threads=4 \
     --db=./db \
     -disable_wal=true \
     -write_buffer_size=67108864 \
     -report_interval_seconds=1 \
     -stats_dump_period_sec=5 \
     --duration=3000 \
     --key_size=16 \
     --value_size=128 \
     --max_write_buffer_number=10 \
     -max_background_compactions=10 \
     -max_background_flushes=10 \
     -subcompactions=4 \
     -compression_type=none \

#cp /mnt/db/LOG ~/fillrandom.log
sleep 20

./db_bench \
     --fs_uri=zenfs://dev:$DEV \
     --benchmarks=overwrite,stats \
     -statistics \
     --num=157797872 \
     --threads=4 \
     --db=./db \
     -disable_wal=true \
     -write_buffer_size=67108864 \
     -report_interval_seconds=1 \
     -stats_dump_period_sec=5 \
     --duration=1800 \
     --key_size=16 \
     --value_size=128 \
     --use_existing_db=1 \
     --use_existing_keys=1 \
     --max_write_buffer_number=10 \
     -max_background_compactions=10 \
     -max_background_flushes=10 \
     -subcompactions=4 \
     -compression_type=none \
