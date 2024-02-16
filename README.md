## About this Repository

This development adds Compaction-Aware Zone Allocation Algorithm to [ZenFS](https://github.com/westerndigitalcorporation/zenfs).

## Compaction-Aware Zone Allocation(CAZA) Overview

CAZA was designed based on how the compaction process selects, merges and invalidates SSTables in the LSM tree. 
As such, CAZA’s design considering the compaction process of LSM-tree maximizes the zone cleaning efficiency of ZenFS by consolidating SSTables with overlapping key ranges located at different levels in the LSM-tree in the same zone and invalidating them together during zone cleaning.

Please refer [Compaction-aware zone allocation for LSM based key-value store on ZNS SSDs](https://discos.sogang.ac.kr/file/2022/intl_conf/HotStorage_2022_H_lee.pdf).

## Dependencies
[libzbd](https://github.com/westerndigitalcorporation/libzbd) and Linux Kernel 5.4 or later

## How to Run

### Build
   ```
   git clone https://github.com/lass-lab/CAZA.git
   ```
   ```
   cd CAZA
   ```
   ```
   make db_bench zenfs
   ```
### Setup ZBD emulation using nullb 
   More about zbd emulation : [zonedstorage.io](https://zonedstorage.io/docs/getting-started/zbd-emulation)
   ```
   # ./setup_zone_nullblk.sh
   ```
### Run
   ```
   # ./run_script.sh
   ```
