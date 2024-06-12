#ifndef MINISQL_FREESPACE_MAP_PAGE_H
#define MINISQL_FREESPACE_MAP_PAGE_H
//
// Created by cactus on 6/11/24.
//

/**
 * Basic freespace_map page format:
 *  ---------------------------------------------------------
 *  | HEADER | pair1<page_id(4),free_space(4)> | ... pair2<page_id(4),free_space(4)> ... |
 *  ---------------------------------------------------------
 *
 *  Header format (size in bytes):
 *  ---------------------------------------------------------------
 *  | PageId (4)| LSN (4) | NextPageId (4)| PairCount(4) |
 *  ---------------------------------------------------------------
 **/
#include <cstring>

#include "common/macros.h"
#include "common/rowid.h"
#include "concurrency/lock_manager.h"
#include "concurrency/txn.h"
#include "page/page.h"
#include "record/row.h"
#include "recovery/log_manager.h"

class FreeSpaceMapPage : public Page {
 public:
  using mappair = std::pair<page_id_t,uint32_t>;
  freespace_map_id_t NewPair(page_id_t page_id, uint32_t free_space);

  void Init(page_id_t page_id, LogManager *log_mgr, Txn *txn);
  inline page_id_t GetNextPageId(){ return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_NEXT_PAGE_ID); }
  inline void SetNextPageId(page_id_t next_page_id){ memcpy(GetData() + OFFSET_NEXT_PAGE_ID ,&next_page_id, sizeof(uint32_t)); }
  inline uint32_t GetPairCount(){ return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_PAIR_COUNT); }
  inline void SetPairCount(uint32_t pair_count){ memcpy(GetData() + OFFSET_PAIR_COUNT ,&pair_count, sizeof(uint32_t)); }
  inline page_id_t GetSpacePageId(freespace_map_id_t internal_id){
    return *reinterpret_cast<page_id_t *>(GetData() + SIZE_FREESPACEMAP_PAGE_HEADER + SIZE_PAIR * internal_id);
  }
  inline void SetSpacePageId(freespace_map_id_t internal_id,page_id_t page_id){
    memcpy(GetData() + SIZE_FREESPACEMAP_PAGE_HEADER + SIZE_PAIR * internal_id, &page_id, SIZE_PAGEID);
  }
  inline uint32_t GetFreeSpace(freespace_map_id_t internal_id){
    return *reinterpret_cast<page_id_t *>(GetData() + SIZE_FREESPACEMAP_PAGE_HEADER + SIZE_PAIR * internal_id + SIZE_PAGEID);
  }
  inline void SetFreeSpace(freespace_map_id_t internal_id,uint32_t free_space){
    memcpy(GetData() + SIZE_FREESPACEMAP_PAGE_HEADER + SIZE_PAIR * internal_id + SIZE_PAGEID, &free_space, SIZE_FREESPACE);
  }

 private:
  static_assert(sizeof(page_id_t) == 4);
  static constexpr size_t SIZE_FREESPACEMAP_PAGE_HEADER = 16;
  static constexpr size_t SIZE_PAIR = sizeof(mappair);
  static constexpr size_t OFFSET_NEXT_PAGE_ID = 8;
  static constexpr size_t OFFSET_PAIR_COUNT = 12;
  static constexpr size_t SIZE_PAGEID = sizeof(page_id_t);
  static constexpr size_t SIZE_FREESPACE = sizeof(uint32_t);
 public:
  static constexpr size_t SIZE_MAX_PAIR = ( PAGE_SIZE - SIZE_FREESPACEMAP_PAGE_HEADER ) / SIZE_PAIR;
};
#endif  // MINISQL_FREESPACE_MAP_PAGE_H
