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

#include "db/db_impl/db_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
//#include "env/io_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class ZenFS;
class DBImpl;
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
    uint64_t start_;
    Zone* zone_;
    std::string fname_;
    Env::WriteLifeTimeHint lt_;

    explicit ZoneExtentInfo(ZoneExtent* extent, ZoneFile* zone_file, bool valid, uint64_t length, uint64_t start, Zone* zone, std::string fname, Env::WriteLifeTimeHint lt) 
        : extent_(extent), 
          zone_file_(zone_file), 
          valid_(valid), 
          length_(length), 
          start_(start), 
          zone_(zone), 
          fname_(fname), 
          lt_(lt){ };
    
    void invalidate() {
        assert(extent_ != nullptr);
        if(!valid_){
            fprintf(stderr, "Try to invalidate invalid extent!\n");
        }       
        valid_ = false;
    };
};

class GCVictimZone {
    public:

     GCVictimZone(Zone* zone, uint64_t invalid_bytes)
         : zone_(zone),
           invalid_bytes_(invalid_bytes){};

     uint64_t get_inval_bytes() const {return invalid_bytes_;};
     Zone * get_zone_ptr() const {return zone_;};

    private:
     Zone *zone_;
     uint64_t invalid_bytes_;
};

class InvalComp{
    public:
        bool operator()(const GCVictimZone *a, const GCVictimZone* b){
            return a->get_inval_bytes() < b->get_inval_bytes();
        };
};

class Zone {
  ZonedBlockDevice *zbd_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z, const uint32_t id);

  std::mutex zone_del_mtx_;
  const uint32_t zone_id_; /* increment from 0 */
  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  bool open_for_write_;
  std::atomic<bool> is_append; /*hold when append*/
  Env::WriteLifeTimeHint lifetime_;
/* weighted average is used only when Allocated for ZC 
 * and corner case in AllocateZone
 * (Corner Case) : All zone has no invalid data but cannot allocate since rough lifetime estimation*/
  double secondary_lifetime_;
  std::atomic<long> used_capacity_;
  std::atomic<bool> zc_victim_;
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
  void Invalidate(ZoneExtent* extent);
 
  void PushExtentInfo(ZoneExtentInfo* extent_info) { 
    extent_info_.push_back(extent_info);
  };

  void UpdateSecondaryLifeTime(Env::WriteLifeTimeHint lt, uint64_t length);
};

class ZonedBlockDevice {
 private:
  std::priority_queue<GCVictimZone *, std::vector<GCVictimZone *>, InvalComp > gc_queue_;
  std::string filename_;
  uint32_t block_sz_;
  uint32_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::vector<Zone *> reserved_zones;

  std::mutex io_zones_mtx;

  std::vector<Zone *> meta_zones;
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
  ZenFS* fs;
  uint64_t reset_cnt;
 public:
  std::atomic<int> append_cnt;
  int num_zc_cnt;
  DBImpl* db_ptr_;
  void SetDBPointer(DBImpl* db);
  std::mutex zone_cleaning_mtx;
  std::vector<ZoneFile *> del_pending;
  std::atomic<bool> zc_in_progress_;
  std::mutex append_mtx_;

  std::mutex df_mtx_;
  std::ofstream df_file;
  std::ofstream reset_file;
  std::atomic<unsigned long long> WR_DATA;
  std::atomic<unsigned long long> LAST_WR_DATA;

  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger);
  virtual ~ZonedBlockDevice();
  
  void printZoneStatus(const std::vector<Zone *>&);
  void SetFsPtr(ZenFS* fss) {fs = fss;}

  IOStatus Open(bool readonly = false);

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime);
  Zone *AllocateZoneForCleaning();
  Zone *AllocateMetaZone();

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

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
  void printZoneExtentInfo(const std::vector<ZoneExtentInfo *>&);

};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
