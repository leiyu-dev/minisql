#ifndef MINISQL_FREESPACE_MAP_H
#define MINISQL_FREESPACE_MAP_H
//
// Created by cactus on 6/11/24.
//
#include "buffer/buffer_pool_manager.h"
#include "concurrency/lock_manager.h"
#include "page/header_page.h"
#include "page/table_page.h"
#include "recovery/log_manager.h"
#include "storage/table_iterator.h"
#include "page/freespace_map_page.h"

class FreeSpaceMap{
 public:
  FreeSpaceMap(page_id_t first_map_page_id,BufferPoolManager* buffer_pool_manager);
  void SetNewPair(page_id_t page_id,uint32_t free_space);
  page_id_t GetBegin(uint32_t need_space);
  page_id_t GetNext(uint32_t need_space);
  page_id_t SetFreeSpace(page_id_t page_id,uint32_t free_space);
  inline page_id_t GetFirstPageId(){ return first_page_id; }
  inline page_id_t GetLastPageId(){ return last_page_id; }

  //only used to debug
  uint32_t GetFreeSpace(page_id_t page_id,freespace_map_id_t internal_index){
    auto page = buffer_pool_manager_->FetchPage(page_id);
    auto freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
    auto pair_count = freespace_map_page->GetPairCount();
    return freespace_map_page->GetFreeSpace(internal_index);

//    for(int i=0;i<pair_count;i++)
//      if(freespace_map_page->GetSpacePageId(i)==page_id)
//    return freespace_map_page->GetFreeSpace(i);
  }
 private:
// public:
  page_id_t first_page_id;
  page_id_t last_page_id;
  BufferPoolManager* buffer_pool_manager_;
  freespace_map_id_t MAX_PAIR_COUNT;

  //iterator
  freespace_map_id_t internal_index;
  page_id_t page_index;
};
#endif  // MINISQL_FREESPACE_MAP_H
