//
// Created by cactus on 6/11/24.
//
#include "storage/freespace_map.h"

FreeSpaceMap::FreeSpaceMap(page_id_t first_map_page_id,BufferPoolManager* buffer_pool_manager): first_page_id(first_map_page_id),
            buffer_pool_manager_(buffer_pool_manager){
  auto page = buffer_pool_manager->FetchPage(first_map_page_id);
  auto freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  //todo:transaction
  freespace_map_page->Init(first_map_page_id, nullptr, nullptr);
  buffer_pool_manager->UnpinPage(first_map_page_id,true);
}

void FreeSpaceMap::SetNewPair(page_id_t page_id,uint32_t free_space){
  Page* page = buffer_pool_manager_->FetchPage(first_page_id);
  if(page== nullptr){
    LOG(ERROR)<<"out of memory"<<std::endl;
    return;
  }
  auto freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  while(true){
    auto pair_count = freespace_map_page->GetPairCount();
    if(pair_count < freespace_map_page->SIZE_MAX_PAIR){
      break;
    }
    page_id_t next_page_id = freespace_map_page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      buffer_pool_manager_->NewPage(next_page_id);
      if(next_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "out of memory" << std::endl;
        return;
      }
      buffer_pool_manager_->UnpinPage(next_page_id,false);
      auto new_page = buffer_pool_manager_->FetchPage(next_page_id);
      auto new_freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(new_page);
      //todo:transaction
      new_freespace_map_page->Init(next_page_id, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(next_page_id,true);
    }
    buffer_pool_manager_->UnpinPage(freespace_map_page->GetPageId(),false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  }
  freespace_map_page->NewPair(page_id, free_space);
}

page_id_t FreeSpaceMap::GetBegin(uint32_t need_space){
  Page* page = buffer_pool_manager_->FetchPage(first_page_id);
  if(page == nullptr){
    LOG(ERROR)<<"out of memory"<<std::endl;
    return INVALID_PAGE_ID;
  }
  auto freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  freespace_map_id_t internal_id = 0;
  while(true){
    bool found = false;
    auto pair_count = freespace_map_page->GetPairCount();
    for(uint32_t i = internal_id;i<pair_count;i++){
      if(freespace_map_page->GetFreeSpace(i)>=need_space){
        internal_id=i;
        found = true;
        break;
      }
    }
    if(found)break;
    internal_id = 0;
    page_id_t next_page_id = freespace_map_page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      buffer_pool_manager_->NewPage(next_page_id);
      if(next_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "out of memory" << std::endl;
        return INVALID_PAGE_ID;
      }
      buffer_pool_manager_->UnpinPage(next_page_id,false);
      auto new_page = buffer_pool_manager_->FetchPage(next_page_id);
      auto new_freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(new_page);
      //todo:transaction
      new_freespace_map_page->Init(next_page_id, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(next_page_id,true);
    }
    buffer_pool_manager_->UnpinPage(freespace_map_page->GetPageId(),false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  }
  internal_index = internal_id;
  page_index = freespace_map_page->GetPageId();
  return freespace_map_page->GetSpacePageId(internal_index);
}

page_id_t FreeSpaceMap::GetNext(uint32_t need_space) {
  Page* page = buffer_pool_manager_->FetchPage(page_index);
  if(page == nullptr){
    LOG(ERROR)<<"out of memory"<<std::endl;
    return INVALID_PAGE_ID;
  }
  auto freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  freespace_map_id_t internal_id = internal_index + 1;
  while(true){
    bool found = false;
    auto pair_count = freespace_map_page->GetPairCount();
    for(uint32_t i=internal_id;i<pair_count;i++){
      if(freespace_map_page->GetFreeSpace(i)>=need_space){
        internal_id=i;
        found = true;
        break;
      }
    }
    if(found)break;
    internal_id = 0;
    page_id_t next_page_id = freespace_map_page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      buffer_pool_manager_->NewPage(next_page_id);
      if(next_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "out of memory" << std::endl;
        return INVALID_PAGE_ID;
      }
      buffer_pool_manager_->UnpinPage(next_page_id,false);
      auto new_page = buffer_pool_manager_->FetchPage(next_page_id);
      auto new_freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(new_page);
      //todo:transaction
      new_freespace_map_page->Init(next_page_id, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(next_page_id,true);
    }
    buffer_pool_manager_->UnpinPage(freespace_map_page->GetPageId(),false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  }
  internal_index = internal_id;
  page_index = freespace_map_page->GetPageId();
  return freespace_map_page->GetSpacePageId(internal_index);
}

page_id_t FreeSpaceMap::SetFreeSpace(page_id_t page_id,uint32_t free_space){
  Page* page = buffer_pool_manager_->FetchPage(first_page_id);
  if(page == nullptr){
    LOG(ERROR)<<"out of memory"<<std::endl;
  return INVALID_PAGE_ID;
  }
  auto freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  freespace_map_id_t internal_id = 0;
  bool found = false;
  while(true){
    found = false;
    auto pair_count = freespace_map_page->GetPairCount();
    for(uint32_t i = internal_id;i<pair_count;i++){
      if(freespace_map_page->GetSpacePageId(i) == page_id){
        internal_id=i;
        found = true;
        break;
      }
    }
    if(found)break;
    internal_id = 0;
    page_id_t next_page_id = freespace_map_page->GetNextPageId();
    if(next_page_id == INVALID_PAGE_ID){
      buffer_pool_manager_->NewPage(next_page_id);
      if(next_page_id == INVALID_PAGE_ID) {
        LOG(ERROR) << "out of memory" << std::endl;
        return INVALID_PAGE_ID;
      }
      buffer_pool_manager_->UnpinPage(next_page_id,false);
      auto new_page = buffer_pool_manager_->FetchPage(next_page_id);
      auto new_freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(new_page);
      //todo:transaction
      new_freespace_map_page->Init(next_page_id, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(next_page_id,true);
    }
    buffer_pool_manager_->UnpinPage(freespace_map_page->GetPageId(),false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    freespace_map_page = reinterpret_cast<FreeSpaceMapPage*>(page);
  }
  if(!found)return INVALID_PAGE_ID;
  freespace_map_page->SetFreeSpace(internal_id,free_space);
  return freespace_map_page->GetPageId();
}