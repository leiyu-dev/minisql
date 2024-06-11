//
// Created by cactus on 6/11/24.
//
#include "page/freespace_map_page.h"

void FreeSpaceMapPage::Init(page_id_t page_id, LogManager *log_mgr, Txn *txn){
  memcpy(GetData(), &page_id, sizeof(page_id));
  SetNextPageId(INVALID_PAGE_ID);
  SetPairCount(0);
}

freespace_map_id_t FreeSpaceMapPage::NewPair(page_id_t page_id, uint32_t free_space){
  auto pair_count = GetPairCount();
  SetFreeSpace(pair_count,free_space);
  SetSpacePageId(pair_count, page_id);
  pair_count++;
  SetPairCount(pair_count);
  return pair_count;
}