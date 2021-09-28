// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN) && defined(LIBZBD)

#include "zbd_zenfs.h"
#include "exp.h"

#include <stdlib.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <string>
#include <utility>
#include <vector>
#include <set>

#include "io_zenfs.h"
#include "rocksdb/env.h"
#include "db/version_set.h"

#define KB (1024)
#define MB (1024 * KB)

using std::cout;
using std::endl;
using std::fixed;

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

/* Reserved Zone for Zone Cleaning, Set as 5 since there are five types of lietime in Rocksdb*/
#define RESERVED_ZONE_FOR_CLEANING (5)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z, const uint32_t id)
    : zbd_(zbd),
      zone_id_(id),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      open_for_write_(false),
      is_append(false){
  lifetime_ = Env::WLTH_NOT_SET;
  secondary_lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));
}

bool Zone::IsUsed() { return (used_capacity_ > 0) || open_for_write_; }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::CloseWR() {

  assert(open_for_write_);
  open_for_write_ = false;
  if (Close().ok()) {
    zbd_->NotifyIOZoneClosed();
  }
  if (capacity_ == 0) zbd_->NotifyIOZoneFull();
}

IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  assert(!IsUsed());

  ret = zbd_reset_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(fd, start_, zone_sz, ZBD_RO_ALL, &z, &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(&z);

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;
  secondary_lifetime_ = Env::WLTH_NOT_SET;

  for(auto ext : extent_info_){
    delete ext;
  }
  extent_info_.clear();
  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_ = start_ + zone_sz;

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int fd = zbd_->GetWriteFD();
  int ret;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = pwrite(fd, ptr, size, wp_);
    if (ret < 0){
        return IOStatus::IOError("Write failed in Zone Append");
    }
    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
  }

  return IOStatus::OK();
}
void Zone::Invalidate(ZoneExtent* extent) {

  bool found = false;
  if (extent == nullptr) {
    fprintf(stderr, "Try to invalidate extent which is nullptr!\n");
  }
  for(const auto ex : extent_info_) {
    if (ex->extent_ == extent) {
      if (found) {
        fprintf(stderr, "Duplicate Extent in Invalidate (%p == %p)\n", ex->extent_, extent);
      }
      ex->invalidate();
      found = true;
    }
  }
  if (!found) {
    fprintf(stderr, "Failed to Find extent in the zone\n");
  }
}

void Zone::UpdateSecondaryLifeTime(Env::WriteLifeTimeHint lt, uint64_t length) {

  uint64_t total_length = 0;
  double slt = 0;
  for (const auto e : extent_info_) {
    total_length += e->length_;
  }
  for(const auto e : extent_info_) {
    double weigth = (((double)e->length_)/(total_length));
    slt += (weigth * (double)(e->lt_));
  }
  double w = (((double)length)/(total_length));
  slt += (w * (double)(lt));
  secondary_lifetime_ = slt;
}

ZoneExtent::ZoneExtent(uint64_t start, uint32_t length, Zone *zone)
    : start_(start), length_(length), zone_(zone) {}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger)
    : filename_("/dev/" + bdevname), logger_(logger), db_ptr_(nullptr) {
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
  zc_in_progress_.store(false);
  WR_DATA.store(0);
  num_zc_cnt = 0;
};

void ZonedBlockDevice::SetDBPointer(DBImpl* db) {
    db_ptr_ = db;
}

IOStatus ZonedBlockDevice::Open(bool readonly) {
  struct zbd_zone *zone_rep;
  unsigned int reported_zones;
  size_t addr_space_sz;
  zbd_info info;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  uint64_t r = 0;
  int ret;
  uint32_t zone_cnt = 0;
  read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device");
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device");
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument("Failed to open zoned block device");
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

  /* We need one open zone for meta data writes, the rest can be used for files
   */
  if (info.max_nr_active_zones == 0)
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones - 1;

  if (info.max_nr_open_zones == 0)
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones - 1;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       info.nr_zones, info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone* new_zone = new Zone(this, z, zone_cnt);
        meta_zones.push_back(new_zone);
        id_to_zone_.insert(std::pair<int,Zone*>(zone_cnt, new_zone));
        zone_cnt++;
      }
      m++;
    }
  }
 
  //(TODO)::Should reserved zone be treated as active_io_zones_?
  while(r <= RESERVED_ZONE_FOR_CLEANING && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone* new_zone = new Zone(this, z, zone_cnt);
        reserved_zones.push_back(new_zone);
        id_to_zone_.insert(std::pair<int,Zone*>(zone_cnt, new_zone));
        zone_cnt++;
      }
      r++;
    }
  }
    
  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z, zone_cnt);
        io_zones.push_back(newZone);
        id_to_zone_.insert(std::pair<int,Zone*>(zone_cnt, newZone));
        zone_cnt++;

        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
            zbd_zone_closed(z)) {
          active_io_zones_++;
          if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
      }
    }
  }

  free(zone_rep);
  start_time_ = time(NULL);

  return IOStatus::OK();
}

void ZonedBlockDevice::NotifyIOZoneFull() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  active_io_zones_--;
  zone_resources_.notify_one();
}

void ZonedBlockDevice::NotifyIOZoneClosed() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  open_io_zones_--;
  zone_resources_.notify_one();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }

  return free;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  io_zones_mtx.lock();

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());

  io_zones_mtx.unlock();
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {

  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  id_to_zone_.clear();
  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);
}

#define LIFETIME_DIFF_NOT_GOOD (100)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

double GetSLifeTimeDiff(const Zone* zone, double zone_secondary_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);
  uint64_t total_length = 0;
  uint64_t expected_length = 0;
  double slt = 0;

  for(const auto e : zone->extent_info_) {
      total_length += e->length_;
  }

  expected_length = (total_length / (zone->extent_info_.size()));
  total_length += expected_length;
  
  for(const auto e : zone->extent_info_) {
      double weigth = (((double)e->length_)/(total_length));
      slt += (weigth * (double)(e->lt_));
  }
  
  double w = (((double)expected_length)/(total_length));
  slt += (w * (double)(file_lifetime));

  if (zone_secondary_lifetime >= slt){
    return zone_secondary_lifetime - slt;
  }    
  
  return slt - zone_secondary_lifetime;
}

Zone *ZonedBlockDevice::AllocateMetaZone() {
  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (!z->IsUsed()) {
      if (!z->IsEmpty()) {
        if (!z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          continue;
        }
      }
      return z;
    }
  }
  return nullptr;
}

void ZonedBlockDevice::ResetUnusedIOZones() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  /* Reset any unused zones */
  for (const auto z : io_zones) {
    if (!z->IsUsed() && !z->IsEmpty()) {
      if (!z->IsFull()) active_io_zones_--;
      if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
    }
  }
}
/*(TODO)
void ZonedBlockDevice::PickZoneWithCompactionVictim(std::vector<Zone*>& candidates) {
 io_zones_mtx should be locked before the function is called 

    std::vector<uint64_t> fno_list;
    db_ptr_->GetCompactionArgs(fno_list);
}
*/
void ZonedBlockDevice::PickZoneWithOnlyInvalid(std::vector<Zone*>& candidates) {
/* io_zones_mtx should be locked before the function is called */
    for (const auto z : io_zones) {
       if ((!z->IsUsed()) && (!z->IsEmpty()) && (!z->IsFull()) ) {
         candidates.push_back(z);
       }
    }
}
void ZonedBlockDevice::SortZone(){

  while (!allocate_queue_.empty()) {
     AllocVictimZone* a = allocate_queue_.top();
     allocate_queue_.pop();
     delete a;
  }
  for (const auto z : io_zones) {
   if(z->IsFull()) continue;

   uint64_t valid_extent_length = 0;
   uint64_t invalid_extent_length = 0;
   
   for (auto ext_info: z->extent_info_) {
    if (ext_info->valid_) {
      uint64_t cur_length = (uint64_t)ext_info->length_;
      uint64_t align = (uint64_t)(cur_length % block_sz_);
      uint64_t pad = 0;
      if (align) {
        pad = block_sz_ - align;
      }
      valid_extent_length += (cur_length + pad);
    } else {
      uint64_t cur_length = (uint64_t)ext_info->length_;
      uint64_t align = (uint64_t)(cur_length % block_sz_);
      uint64_t pad = 0;
      if (align) {
        pad = block_sz_ - align;
      }
      invalid_extent_length += (cur_length + pad);
    }
   }
   //Insert into queue with sorting by its invalid ratio. 
   ///Higher the invalid ratio, Higher the priority.
   allocate_queue_.push(new AllocVictimZone(z, invalid_extent_length, valid_extent_length));
 }
 assert(!allocate_queue_.empty());
};

Zone* ZonedBlockDevice::AllocateMostL0L1Files(const std::set<int>& zone_list) {

    uint64_t max = 0;
    Zone* z = nullptr;

    if (zone_list.empty()) 
      return z;

    for (const auto z_id : zone_list) {
      Zone* zone = id_to_zone_.find(z_id)->second;
      uint64_t length = 0;
      
      if (!zone->open_for_write_ && !zone->IsFull()) {

        for (const auto& ext : zone->extent_info_) {
          if (ext->level_ == 0 || ext->level_ == 1) {
            if (ext->valid_) {
              length += ext->length_;  
            }
          }
        } 
        if (length > max){ 
          max = length;
          z = zone;
        }
      }
    }
    return z;
}

void ZonedBlockDevice::SameLevelFileList(const int level, std::vector<uint64_t>& fno_list){
    db_ptr_->SameLevelFileList(level, fno_list);
}

void ZonedBlockDevice::AdjacentFileList(const InternalKey& s, const InternalKey& l, const int level, std::vector<uint64_t>& fno_list){
    if(level == -99) return;
    db_ptr_->AdjacentFileList(s, l, level, fno_list);
}
/*
void ZonedBlockDevice::FindClosestFilesWithSameLevel(const int level, std::vector<uint64_t>& fno_list){

    db_ptr_->FindClosestFilesWithSameLevel(level, fno_list);
}
*/
Zone *ZonedBlockDevice::AllocateZone(InternalKey smallest, InternalKey largest, 
                                    int level, Env::WriteLifeTimeHint file_lifetime, uint64_t fnum) {

  Zone *allocated_zone = nullptr;
  //Zone *finish_victim = nullptr;
  //unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  int cases = 0;
  Status s;
  
  io_zones_mtx.lock();
  //std::cerr<<"Allocate : "<<fnum<<" with level("<<level<<")"<<std::endl;
  /* Make sure we are below the zone open limit */
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this] {
      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
      return false;
    });
  }
  /* Sort Zone by follows rules
   * (1) has less valid data
   * (2) has more invalid data 
   * (3) open_for_write_ is false */
  SortZone();
 
  /* Reset any unused zones and finish used zones under capacity treshold*/
  for (const auto z : io_zones) {
    if (z->open_for_write_ || z->IsEmpty() || (z->IsFull() && z->IsUsed()))
      continue;
    
    if (!z->IsUsed() && z->IsFull()) {
      bool all_invalid = true;
      
      for (auto exinfo : z->extent_info_) {
        if (exinfo->valid_ == true) {
          all_invalid = false;
        }
      }
      assert(all_invalid);
      s = z->Reset();

      if (!s.ok()) {
        Debug(logger_, "Failed resetting zone !");
      }
      continue;
    }
    
    if ((z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100))) {
      /* If there is less than finish_threshold_% remaining capacity in a
       * non-open-zone, finish the zone */
      s = z->Finish();
      if (!s.ok()) {
        Debug(logger_, "Failed finishing zone");
      }
      active_io_zones_--;
    }
  }
  if (sst_to_zone_.empty()) {
    if (active_io_zones_.load() < max_nr_active_io_zones_) {
      for (const auto z : io_zones) {
       /* Find the Empty Zone First */ 
        if ((!z->open_for_write_) && z->IsEmpty()) {
          z->lifetime_ = file_lifetime;
          allocated_zone = z;
          active_io_zones_++;
          new_zone = 1;
          break;
        }
      }
    }
  }
  if (allocated_zone) {
    assert(!allocated_zone->open_for_write_);
    allocated_zone->open_for_write_ = true;
    open_io_zones_++;
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
    io_zones_mtx.unlock();
    return allocated_zone;
  }
  assert(!allocated_zone);
  // There's valid SSTables in Zones
  // Find zone where the files located at adjacent level and having overlapping keys
  std::vector<uint64_t> fno_list;
  std::vector<Zone*> candidates;
  AdjacentFileList(smallest, largest, level, fno_list);
  
  if (!fno_list.empty()) {
   /* There are SSTables with overlapped keys and adjacent level.
      (1) Find the Zone where the SSTables are written */
    std::set<int> zone_list;
    for (uint64_t fno : fno_list) {
      auto z = sst_to_zone_.find(fno);
      //assert(z != sst_to_zone_.end());
      if (z != sst_to_zone_.end()) {
        //std::cerr << "Searching overlapped fno (" << fno <<") is in zone id : ";
        for (int zone_id : z->second) {
          //std::cerr  << zone_id << " ";
          zone_list.insert(zone_id);
        }
        //std::cerr << std::endl;
      }
    }
    //(2) Pick the Zones with free space as candidates
    for (const auto z : io_zones) {
      auto search = zone_list.find(z->zone_id_);
      if (search != zone_list.end()){
        if (!z->IsFull()) {
          candidates.push_back(z);
        }
      }
    }
    if (candidates.empty()) {
      /* All Zones with overlapped SSTable are full */
      /* In this case, 
       * First) : Find the Zone that has only Invalid data */
      PickZoneWithOnlyInvalid(candidates);

      if (!candidates.empty()) {
        /* If there are Zone with only Invalid data and has free space */
        // Pick the Zone with least Free space.
        size_t min_free_space = candidates[0]->max_capacity_; 
        for (const auto z : candidates) {
          size_t cur_free = z->capacity_;
          if (cur_free < min_free_space && !z->open_for_write_) {
            min_free_space = cur_free;
            allocated_zone = z;
            cases = 1;
          }
        }
      } else {
        /*There is no Zone only filled with invalid data and has free space*/
        /* (TODO) Pick the Zone with Files that is compacted soon 
           For this, We need to Check the Compaction Argument list */
        //For now, just allocate with most invalid data and least valid data
        while (!allocate_queue_.empty()) {
          Zone* z =  allocate_queue_.top()->get_zone_ptr();
          if (!z->open_for_write_ && !z->IsFull() ) {
            allocated_zone = z;
            cases = 2;
            break;
          }
          allocate_queue_.pop();
        }
      }
    } else { 
      /* There is at least one zone having free space */
      /* If there's more than one zone, pick the zone with most invalid data */
      while (!allocate_queue_.empty()) {
        AllocVictimZone* az = allocate_queue_.top();
        Zone* cz = az->get_zone_ptr(); 
        for ( const auto z : candidates ) {
           if (z == cz && (!cz->open_for_write_)) {
             allocated_zone = z;
             cases = 3;
             break;
           } 
        }
        allocate_queue_.pop();
      }
    }
  } else {
  /* (1) There is no matching files being overlapped with current file
        ->(TODO)Find the file within the same zone which has smallest key diff
     (2) There is no propriate zone to allocate 
        ->(a) Try to allocate zone with Free Zone.
        ->(b) Try to allocate zone with most least valid data. */
    if (level == 1 || level == 0) {
      std::set<int> zone_list;
      // L0 and L1 files are often compacted altogether.
      // Allocate to the zone where other L0 or L1 files are located.
      SameLevelFileList(0, fno_list);
      SameLevelFileList(1, fno_list);

      for (uint64_t fno : fno_list) {
        auto z = sst_to_zone_.find(fno);
        if (z != sst_to_zone_.end()) {
          for (int zone_id : z->second) {
            zone_list.insert(zone_id);
          }
        }
      }
      //Allocate Zones with most the number of l0, l1 files
      allocated_zone = AllocateMostL0L1Files(zone_list);
      if (allocated_zone) cases = 4;
      if (allocated_zone == nullptr && 
              active_io_zones_.load() < max_nr_active_io_zones_) {
        for (const auto z : io_zones) {
          if ((!z->open_for_write_) && z->IsEmpty()) {
            allocated_zone = z;
            cases = 5;
            active_io_zones_++;
            new_zone = 1;
            break;
        }
       }
     } 
    } else {
    // The output level is > 1 
    // Try to Allocate to the zone with empty space.
      if (active_io_zones_.load() < max_nr_active_io_zones_) {
        for (const auto z : io_zones) {
          if ((!z->open_for_write_) && z->IsEmpty()) {
            allocated_zone = z;
            cases = 6;
            active_io_zones_++;
            new_zone = 1;
            break;
        }
       }
      }
      if (!allocated_zone) {
        //Try to Allocate Zone with only invalid data
        for (const auto z : io_zones) {
          if ((!z->IsUsed()) && (!z->IsEmpty()) && (!z->IsFull()) ) {
           allocated_zone = z;
           cases = 7;
           break;
          }
        }
      }
    }
  }
  if (!allocated_zone) {
    SortZone();
    while (!allocate_queue_.empty()) {
      Zone* z = allocate_queue_.top()->get_zone_ptr();
      if (!z->open_for_write_ && !z->IsFull()) {
          allocated_zone = z;
          cases = 8;
          break;
      }
      allocate_queue_.pop();
    }
  }
//  cases +=0;
  if (allocated_zone) {
    std::cerr<<"Allocate SST File ("<<fnum<<") with level("<<level<<") to zone ("<<allocated_zone->zone_id_<<") with case("<<cases<<")"<<std::endl;
    assert(!allocated_zone->open_for_write_);
    allocated_zone->open_for_write_ = true;
    open_io_zones_++;
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
    io_zones_mtx.unlock();
    return allocated_zone;
  }

  if (!allocated_zone) {
  //Trigger GC for reclaim free space in the Device.
  //(Step 1) Classify all active zones by its invalid data ratio.
#ifdef EXPERIMENT
        cout << "GC triggered count : " << ++num_zc_cnt <<"Time : " << db_ptr_->GetTimeStamp() <<endl;
#endif
    for (auto z : io_zones) {
        z->zone_del_mtx_.lock();
        uint64_t valid_extent_length = 0;
        uint64_t invalid_extent_length = 0;
        std::vector<ZoneExtentInfo *> invalid_list;
        std::vector<ZoneExtentInfo *> valid_list;
        for(auto ext_info: z->extent_info_) {
            /*
              Busy wait til Append request to the zone completed.
              No need to check the condition in the loop 
              since zone is allocated to one file at each time.
            */
            while(z->is_append.load()){ }
            
            if(ext_info->valid_) {
                uint64_t cur_length = (uint64_t)ext_info->length_;
                uint64_t align = (uint64_t)(cur_length % block_sz_);
                uint64_t pad = 0;
                if(align){
                    pad = block_sz_ - align;
                }
                valid_extent_length += (cur_length + pad);
                valid_list.push_back(ext_info);
            }else {
                uint64_t cur_length = (uint64_t)ext_info->length_;
                uint64_t align = (uint64_t)(cur_length % block_sz_);
                uint64_t pad = 0;
                if(align){
                    pad = block_sz_ - align;
                }
                invalid_extent_length += (cur_length + pad);
                invalid_list.push_back(ext_info);
            }
        }

#ifdef EXPERIMENT
        cout << "Zone Information" <<endl;
        cout << "start : " << z->start_<< fixed <<endl;
        cout << "wp : " << z->wp_ << fixed <<endl;
        cout << "used_capcity : " << z->used_capacity_.load() << endl;
        cout << "max_capacity : " << z->max_capacity_ << endl;
        cout << "invalid_bytes(with_padds) : " << invalid_extent_length <<endl;
        cout << "valid_bytes(with_padds) : " << valid_extent_length  <<endl;
        cout << "remain_capacity : " << z->capacity_ <<endl;
        cout << "lifetime : " << z->lifetime_ << endl;
        cout << "invalid_ext_cnt : " << invalid_list.size() << endl;  
        cout << "valid_ext_cnt : " << valid_list.size() << endl;  
        cout << "Invalid Extent Information" << endl;
        printZoneExtentInfo(invalid_list);
        cout << "Valid Extent Information" << endl;
        printZoneExtentInfo(valid_list);
#endif 
        //Insert into queue with sorting by its invalid ratio. 
        //Higher the invalid ratio, Higher the priority.
        gc_queue_.push(new GCVictimZone(z, invalid_extent_length));
        z->zone_del_mtx_.unlock();
    }

  db_ptr_->compaction_input_mutex_.lock();
  db_ptr_->printCompactionHistory();
  db_ptr_->CloseLSMHistoryFile();
  db_ptr_->compaction_input_mutex_.unlock();
  cout << "Total Copied data in ZC : 9999999" << endl;
  std::cerr<<"System going dead with fno("<<fnum<<") with level("<<level<<")"<<std::endl;
  exit(1);
  
  }
    
  ZoneCleaning();

  io_zones_mtx.unlock();
  LogZoneStats();
  
  if(!allocated_zone){
    fprintf(stderr, "Allocate Zone Failed!\n");
    db_ptr_->compaction_input_mutex_.lock();
    db_ptr_->printCompactionHistory();
    db_ptr_->CloseLSMHistoryFile();
    db_ptr_->compaction_input_mutex_.unlock();
    exit(1);
  }
  return allocated_zone;
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }
uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

Zone *ZonedBlockDevice::AllocateZoneForCleaning(std::vector<Zone *> new_io_zones, Env::WriteLifeTimeHint lt) {

  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Status s;

  assert(!new_io_zones.empty());
  /* Make sure we are below the zone open limit */
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    zone_resources_.wait(lk, [this] {
      if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
      return false;
    });
  }

  /* Try to fill an already open zone(with the best life time diff) */
  for (const auto z : new_io_zones) {
      if ((!z->open_for_write_) && (z->used_capacity_ > 0) && !z->IsFull()) {
          unsigned int diff = GetLifeTimeDiff(z->lifetime_, lt);
          if (diff <= best_diff) {
              allocated_zone = z;
              best_diff = diff;
          }
      }
  }

  if (best_diff < LIFETIME_DIFF_NOT_GOOD) {
      assert(!allocated_zone->open_for_write_);
      allocated_zone->open_for_write_ = true;
      open_io_zones_++;
      return allocated_zone;
  }

  /* If we did not find a good match, just allocate with least free space 
     to provide more free space after Zone Cleaning */
  allocated_zone = nullptr;
  uint64_t least_free_space = (new_io_zones[0]->max_capacity_);
  for (const auto z : new_io_zones) {
      uint64_t free_space = (z->max_capacity_ - (z->wp_ - z->start_));
      if ((least_free_space >= free_space) && (free_space != 0) && (!z->open_for_write_)) {
          least_free_space = free_space;
          allocated_zone = z;
      }
  }
    
  if (!allocated_zone) {

      printZoneStatus(new_io_zones);
      fprintf(stderr, "Allocate Zone Failed While Running Zone Cleaning!\n");
      db_ptr_->compaction_input_mutex_.lock();
      db_ptr_->printCompactionHistory();
      db_ptr_->CloseLSMHistoryFile();
      db_ptr_->compaction_input_mutex_.unlock();
      exit(1);
  }
  assert(!allocated_zone->open_for_write_);
  allocated_zone->open_for_write_ = true;
  open_io_zones_++;

  return allocated_zone;
}

void ZonedBlockDevice::printZoneStatus(const std::vector<Zone *>& zones){

    for (auto z : zones) {
        
        fprintf(stderr, "start : %ld\n", z->start_);
        fprintf(stderr, "wp_ : %ld\n", z->wp_);
        fprintf(stderr, "capacity_ : %ld\n", z->capacity_);
        fprintf(stderr, "used_capacity_ : %ld\n", z->used_capacity_.load());

        if(z->open_for_write_) {
            fprintf(stderr, "open_for_write_ : true\n");
        }else{
            fprintf(stderr, "open_for_write_ : false\n");
        }
        
        if(z->IsUsed()) {
            fprintf(stderr, "is_used : true\n");
        }else{
            fprintf(stderr, "is_used : false\n");
        }

        if(z->IsFull()) {
            fprintf(stderr, "is_full : true\n");
        }else{
            fprintf(stderr, "is_full : false\n");
        }

        if(z->IsEmpty()) {
            fprintf(stderr, "is_empty : true\n");
        }else{
            fprintf(stderr, "is_empty : false\n");
        }
        fprintf(stderr, "\n\n");
    }

}
/*
 ZoneCleaning
 (1) Select zone with most invalid data.
 (2) Process until every invalid data gets cleaned from zone.
*/
int ZonedBlockDevice::ZoneCleaning() {
 
    assert(zc_in_progress_.load() == false);

    zone_cleaning_mtx.lock();
    zc_in_progress_.store(true);

    size_t prev_io_zone_cnt = io_zones.size();
    size_t prev_reserved_zone_cnt = reserved_zones.size();
#ifdef EXPERIMENT
    uint64_t copied_data = 0;
#endif
    std::vector<Zone *> new_io_zones = reserved_zones;
    for(Zone * nz : new_io_zones){
        nz->Reset();
    }
    std::vector<Zone *> used_io_zones;
    Zone* allocated_zone = nullptr;
    
    while(!gc_queue_.empty()){

        //Process until every invalid data gets cleaned from zone.
        Zone* cur_victim = gc_queue_.top()->get_zone_ptr();
        assert(cur_victim);
        
        uint64_t iinvalid_ratio = gc_queue_.top()->get_inval_bytes();
        
        if(cur_victim->open_for_write_) {
            //Do not touch currently used zone
            used_io_zones.push_back(cur_victim);
            gc_queue_.pop();
            continue;
        }

        if(iinvalid_ratio == 0){
            new_io_zones.push_back(cur_victim);
            gc_queue_.pop();
            continue; 
        }

        //Find the valid extents in currently selected zone.
        //Should recognize which file each extent belongs to.
        std::vector<ZoneExtentInfo *> valid_extents_info;

        for(auto exinfo : cur_victim->extent_info_){

            if(exinfo->valid_ == true) {
                valid_extents_info.push_back(exinfo);
            }
        }

        if(valid_extents_info.empty()){
            new_io_zones.push_back(cur_victim);
            gc_queue_.pop();
            continue;
        }
         //(1) Find which ZoneFile current extents belongs to.
         //(2) Check Each lifetime of file to which each extent belongs to.    
        for(ZoneExtentInfo* ext_info : valid_extents_info) {
            //Extract All the inforamtion from Extents inforamtion structure
            assert(cur_victim == ext_info->extent_->zone_);
            ZoneExtent* zone_extent = ext_info->extent_;

            ZoneFile* zone_file = ext_info->zone_file_;

            Env::WriteLifeTimeHint lt = zone_file->GetWriteLifeTimeHint();

            assert(zone_extent && zone_file);

            //extract the contents of the current extent
            uint32_t extent_length = zone_extent->length_; 
            
            uint32_t pad_sz = 0;
            uint32_t align = extent_length % block_sz_;
            uint32_t content_length = extent_length; 

            if(align){
                uint32_t block_nr = (content_length / block_sz_) + 1;
                content_length = block_sz_ * block_nr;
                pad_sz = block_sz_ - align; 
            }

            char* content = (char *)malloc(sizeof(char)*content_length);

            int f = GetReadFD();
            int f_direct = GetReadDirectFD();
            ssize_t r = 0;
            uint64_t r_off = zone_extent->start_;

            r = pread(f, content, zone_extent->length_, r_off);
            
            if( r < 0 ) {
                r = pread(f_direct, content, zone_extent->length_, r_off);
            }
            
            assert(r >= 0);
            
            if(pad_sz != 0) {
                memset((char*)content + extent_length, 0x0, pad_sz); 
            }

            //allocate Zone and write contents.
            allocated_zone = AllocateZoneForCleaning(new_io_zones, lt);
            
            //Copy contents to new zone.
            assert(allocated_zone);
            {
                IOStatus s;
                uint32_t left = content_length;
                
                std::vector<ZoneExtent *> new_zone_extents;
                //Append the data to the allocated zone.
                uint32_t wr_size = 0;
                uint32_t offset = 0;

                while (left) { 
                    assert(allocated_zone);
                   
                    if(left <= allocated_zone->capacity_){

                    //There'are enough room for write original extent
                        s = allocated_zone->Append((char*)content + offset, left);
#ifdef EXPERIMENT
                        copied_data += (uint64_t)left;
#endif
                        allocated_zone->used_capacity_ += left;

                        ZoneExtent * new_extent = new ZoneExtent((allocated_zone->wp_ - left), /*Extent length*/left, allocated_zone);
                        ZoneExtentInfo * new_extent_info = new ZoneExtentInfo(new_extent, zone_file ,true, left, new_extent->start_, allocated_zone, zone_file->GetFilename(), zone_file->GetWriteLifeTimeHint(), zone_file->level_);
                        allocated_zone->UpdateSecondaryLifeTime(zone_file->GetWriteLifeTimeHint(), left);
                        allocated_zone->PushExtentInfo(new_extent_info);
                        new_zone_extents.push_back(new_extent);
                        break; /*left = 0*/
                    } 
                    
                    //There are not enough room for write orinal extent. 
                    //Break original extent into multiple extents. 
                   
                    wr_size = left;
                    
                    if(wr_size > allocated_zone->capacity_) { 
                        wr_size = allocated_zone->capacity_;

                        s = allocated_zone->Append((char*)content + offset, wr_size);

                        assert(s.ok()); 
#ifdef EXPERIMENT
                        copied_data += (uint64_t)wr_size;
#endif
                        allocated_zone->used_capacity_ += wr_size;

                        left -= wr_size;
                        offset += wr_size;

                        assert(allocated_zone->capacity_ == 0); 

                        ZoneExtent * new_extent = new ZoneExtent((allocated_zone->wp_ - wr_size), /*Extent length*/wr_size, allocated_zone);

                        ZoneExtentInfo * new_extent_info = new ZoneExtentInfo(new_extent, zone_file ,true, wr_size,new_extent->start_,allocated_zone, zone_file->GetFilename(), zone_file->GetWriteLifeTimeHint(), zone_file->level_);
                        allocated_zone->UpdateSecondaryLifeTime(zone_file->GetWriteLifeTimeHint(), left); 
                        allocated_zone->PushExtentInfo(new_extent_info);    
                        new_zone_extents.push_back(new_extent);

                        //update and notify resource status
                        allocated_zone->open_for_write_ = false;
                        open_io_zones_--;
                        
                        allocated_zone->Finish();
                        active_io_zones_--;
    
                        //newly allocate new zone for write
                        allocated_zone = AllocateZoneForCleaning(new_io_zones, lt);
                        assert(allocated_zone);
                    }
                }//end of while.
            
                allocated_zone->open_for_write_ = false;    
                open_io_zones_--;
                assert(cur_victim->used_capacity_ >= (long)extent_length); 
                cur_victim->used_capacity_ -= (long)extent_length; 
                //update extent information of the file.
                //Replace origin extent information with newly made extent list.
                std::vector<ZoneExtent *> origin_extents_ = zone_file->GetExtentsList();
                std::vector<ZoneExtent *> replace_extents_;
                for(auto ze : origin_extents_) {
                    if(zone_extent == ze) {
                        for(auto new_ze : new_zone_extents) {
                            replace_extents_.push_back(new_ze);
                        }
                    }else {
                       replace_extents_.push_back(ze);
                    }
                }
                zone_file->UpdateExtents(replace_extents_);
            }            
            free(content);
        }
        assert(!(cur_victim->open_for_write_));
        cur_victim->used_capacity_.store(0);
        cur_victim->Reset();
        active_io_zones_--;
        new_io_zones.push_back(cur_victim);
        gc_queue_.pop();
       
        for(auto zit = used_io_zones.begin(); zit != used_io_zones.end();){
            if((*zit)->open_for_write_ == false){
                uint64_t invalid_extent_length = 0;

                for(auto ext_info : (*zit)->extent_info_) {
                
                    if(ext_info->valid_ == false){
                        uint64_t cur_length = (uint64_t)ext_info->length_;
                        uint64_t align = (uint64_t)(cur_length % block_sz_);
                        uint64_t pad = 0;

                        if(align){
                            pad = block_sz_ - align;
                        }
                        invalid_extent_length += (cur_length + pad);
                    }
                }
                
                //Insert into queue with sorting by its invalid ratio. 
                //Higher the invalid ratio, Higher the priority.
                gc_queue_.push(new GCVictimZone((*zit), invalid_extent_length));
                used_io_zones.erase(zit);
            }else{
                zit++;
            }
        }
    }
    std::vector<Zone *> new_reserved_io_zones;
    
    int cnt = 0;
    bool is_picked;

    for(auto nz : new_io_zones) {
        if( cnt <= RESERVED_ZONE_FOR_CLEANING && nz->IsEmpty() && !(nz->IsUsed())) {
            new_reserved_io_zones.push_back(nz);
            cnt++;
        } 
    }
    io_zones.clear();
    for(auto z : new_io_zones){
        is_picked = false;
        for(auto rz : new_reserved_io_zones) {
            if( z == rz) {
                is_picked = true;
                break;
            }
        }
        if(!is_picked)
            io_zones.push_back(z);
    }
    for(auto uz : used_io_zones){
        io_zones.push_back(uz);
    }
    used_io_zones.clear();
    reserved_zones.clear();
    reserved_zones = new_reserved_io_zones;

    assert((prev_io_zone_cnt + prev_reserved_zone_cnt) == (io_zones.size() + reserved_zones.size()));
    zc_in_progress_.store(false);
    zone_cleaning_mtx.unlock();
#ifdef EXPERIMENT
    fprintf(stdout, "Total Copied Data in ZC : %lu\n", copied_data);
#endif
    return 1;
}//ZoneCleaning();

void ZonedBlockDevice::printZoneExtentInfo(const std::vector<ZoneExtentInfo *>& list) {

    //print each extents file name and legnth
    for(const auto &ext : list){
        cout<< "file name : " << ext->fname_ << endl;
        cout<< "extent length : " << ext->length_ << endl;

    }

}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN) && defined(LIBZBD)
