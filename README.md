## About this branch

This development branch adds ZenFS, a storage backend for Zoned Block Devices, to RocksDB.
This branch will reflect the latest state of the implementation until the code has been merged.
Expect regular rebases.
 
## ZenFS Overview

ZenFS is a simple file system that utilizes RockDBs FileSystem interface to place files into zones on a raw zoned block device. By separating files into zones and utilizing the write life time hints to co-locate data of similar life times, the system write amplification is greatly reduced(compared to conventional block devices) while keeping the ZenFS capacity overhead at a very reasonable level.

ZenFS is designed to work with host-managed zoned spinning disks as well as NVME SSDs with Zoned Namespaces.

Some of the ideas and concepts in ZenFS are based on earlier work done by Abutalib Aghayev and Marc Acosta.

### Dependencies

ZenFS depends on[ libzbd ](https://github.com/westerndigitalcorporation/libzbd) and Linux kernel 5.4 or later to perform zone management operations.

## Architecture overview
![zenfs stack](https://user-images.githubusercontent.com/447288/84152469-fa3d6300-aa64-11ea-87c4-8a6653bb9d22.png)

ZenFS implements the FileSystem API, and stores all data files on to a raw zoned block device. Log and lock files are stored on the default file system under a configurable directory. Zone management is done through libzbd and zenfs io is done through normal pread/pwrite calls.

Optimizing the IO path is on the TODO list.

## Example usage

This example issues 100 million random inserts followed by as many overwrites on a 100G memory backed zoned null block device. Target file sizes are set up to align with zone size.
Please report any issues.


```
make db_bench zenfs

sudo su

./setup_zone_nullblk.sh

DEV=nullb1
ZONE_CAP_SECS=$(blkzone report -c 5 /dev/$FS_PATH | grep -oP '(?<=cap )[0-9xa-f]+' | head -1)
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

./db_bench --fs_uri=zenfs://$DEV --key_size=16 --value_size=800 --target_file_size_base=$TARGET_FZ_BASE --write_buffer_size=$WB_SIZE --max_bytes_for_level_base=$MAX_BYTES_FOR_LEVEL_BASE --max_bytes_for_level_multiplier=4 --use_direct_io_for_flush_and_compaction --max_background_jobs=$(nproc) --num=100000000 --benchmarks=fillrandom,overwrite
```

This graph below shows the capacity usage over time.
As ZenFS does not do any garbage collection the write amplification is 1.

![zenfs_capacity](https://user-images.githubusercontent.com/447288/84157574-2c51c380-aa6b-11ea-89e2-def4a4d658af.png)


## File system implementation

Files are mapped into into a set of extents:

* Extents are block-aligned, continious regions on the block device
* Extents do not span across zones
* A zone may contain more than one extent
* Extents from different files may share zones

### Reclaim 
ZenFS is exceptionally lazy at current state of implementation and does not do any garbage collection whatsoever. As files gets deleted, the used capacity zone counters drops and when
it reaches zero, a zone can be reset and reused.

###  Metadata 
Metadata is stored in a rolling log in the first zones of the block device.

Each valid meta data zone contains:

* A superblock with the current sequence number and global file system metadata
* At least one snapshot of all files in the file system
    
**The metadata format is currently experimental.** More extensive testing is needed and support for differential updates is planned to be implemented before bumping up the version to 1.0.


## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)
[![TravisCI Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)
[![Appveyor Build status](https://ci.appveyor.com/api/projects/status/fbgfu0so3afcno78/branch/master?svg=true)](https://ci.appveyor.com/project/Facebook/rocksdb/branch/master)
[![PPC64le Build Status](http://140-211-168-68-openstack.osuosl.org:8080/buildStatus/icon?job=rocksdb&style=plastic)](http://140-211-168-68-openstack.osuosl.org:8080/job/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/ and https://rocksdb.slack.com/

## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
