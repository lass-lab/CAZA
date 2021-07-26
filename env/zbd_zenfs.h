// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include <queue>
#include <functional>

#include <iostream>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class Zone;
class ZoneFile;
class ZonedBlockDevice;
class ZoneExtent;

//(ZC)::class and struct added for Zone Cleaning 
struct ZoneExtentInfo {

    ZoneExtent* extent_;
    ZoneFile* zone_file_;
    bool valid_;
    uint32_t length_;

    ZoneExtentInfo(ZoneExtent* extent, ZoneFile* zone_file, bool valid, uint32_t length) 
        : extent_(extent), zone_file_(zone_file), valid_(valid), length_(length) {};
    
    void invalidate() {
        assert(extent_ != nullptr);
        assert(valid_);
        valid_ = false;
    };
};

class GCVictimZone {
    public:

     GCVictimZone(Zone* zone, double invalid_ratio)
         : zone_(zone),
           invalid_ratio_(invalid_ratio){};

     double get_inval_ratio() const {return invalid_ratio_;};
     Zone * get_zone_ptr() const {return zone_;};

    private:
     Zone *zone_;
     double invalid_ratio_;
};

class InvalComp{
    public:
        bool operator()(const GCVictimZone *a, const GCVictimZone* b){
            return a->get_inval_ratio() < b->get_inval_ratio();
        };
};

class Zone {
  ZonedBlockDevice *zbd_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);

  std::mutex zone_mtx_;
  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  bool open_for_write_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<long> used_capacity_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  //list of extents lives in here.
  std::vector<ZoneExtentInfo *> extent_info_;
  void CloseWR(); /* Done writing */
  void PushExtentInfo(ZoneExtentInfo* extent_info) { 
    extent_info_.push_back(extent_info);
  };
  
  void Invalidate(ZoneExtent* extent) {
    for(auto ex : extent_info_) {
        if (ex->extent_ == extent) {
            if(extent == nullptr){
                fprintf(stderr, "Try to innvalidate extent which is nullptr!\n");
            }
            ex->invalidate();
        }
    }
  };

};

class ZonedBlockDevice {
 private:
  std::priority_queue<GCVictimZone *, std::vector<GCVictimZone *>, InvalComp > gc_queue_;
  std::string filename_;
  uint32_t block_sz_;
  uint32_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::mutex io_zones_mtx;

  std::vector<Zone *> meta_zones;
  std::vector<Zone *> reserved_zones; // reserved for a Zone Cleaning
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::condition_variable zone_resources_;
  std::mutex zone_resources_mtx_; /* Protects active/open io zones */

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;
 
 public:
  std::mutex zone_cleaning_mtx;
  std::vector<ZoneFile *> del_pending;
  std::atomic<bool> zc_in_progress_;
  std::mutex append_mtx_;

  unsigned long long WR_DATA;

  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger);
  virtual ~ZonedBlockDevice();
  
  void printZoneStatus(const std::vector<Zone *>);

  IOStatus Open(bool readonly = false);

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime);
  Zone *AllocateZoneForCleaning(std::vector<Zone *>& new_io_zones, Env::WriteLifeTimeHint lifetime);
  Zone *AllocateMetaZone();

  uint64_t GetFreeSpace();
  std::string GetFilename();
  uint32_t GetBlockSize();

  void ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();
  
  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint32_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void NotifyIOZoneFull();
  void NotifyIOZoneClosed();

  int ZoneCleaning();
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
