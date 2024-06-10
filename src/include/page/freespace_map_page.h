#ifndef MINISQL_FREESPACE_MAP_PAGE_H
#define MINISQL_FREESPACE_MAP_PAGE_H
//
// Created by cactus on 6/11/24.
//

/**
 * Basic freespace_map page format:
 *  ---------------------------------------------------------
 *  | HEADER | pair1<free_space(4),page_id(4)> | ... pair2<free_space(4),page_id(4)> ... |
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

  void Init(page_id_t page_id, LogManager *log_mgr, Txn *txn);

  bool SetFreeSpace(table_internal_id_t internal_id, uint32_t free_space);

  //return INVALID_PAGE_ID if it can't find a page
  uint32_t GetFreeSpace();

  page_id_t GetValidPage();

 private:
  uint32_t GetNextPageId(){ return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_NEXT_PAGE_ID); }
  void SetNextPageId(uint32_t next_page_id){ memcpy(GetData() + OFFSET_NEXT_PAGE_ID ,&next_page_id, sizeof(uint32_t)); }
  uint32_t GetPairCount(){ return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_PAIR_COUNT); }
  void SetPairCount(uint32_t pair_count){ memcpy(GetData() + OFFSET_PAIR_COUNT ,&pair_count, sizeof(uint32_t)); }
 private:
  static_assert(sizeof(page_id_t) == 4);
  static constexpr size_t SIZE_FREESPACEMAP_PAGE_HEADER = 16;
  static constexpr size_t SIZE_PAIR = sizeof(std::pair<uint32_t,page_id_t>);
  static constexpr size_t OFFSET_NEXT_PAGE_ID = 8;
  static constexpr size_t OFFSET_PAIR_COUNT = 12;
 public:
  static constexpr size_t SIZE_MAX_PAIR = ( PAGE_SIZE - SIZE_FREESPACEMAP_PAGE_HEADER ) / SIZE_PAIR;
};
#endif  // MINISQL_FREESPACE_MAP_PAGE_H
