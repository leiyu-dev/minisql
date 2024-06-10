//
// Created by cactus on 6/11/24.
//
#include "page/freespace_map_page.h"

void FreeSpaceMapPage::Init(page_id_t page_id, LogManager *log_mgr, Txn *txn){
  memcpy(GetData(), &page_id, sizeof(page_id));
  SetNextPageId(INVALID_PAGE_ID);
  SetPairCount(0);
}

bool FreeSpaceMapPage::SetFreeSpace(table_internal_id_t internal_id, uint32_t free_space){
  int pair_count = GetPairCount();
  for(int i=0;i<pair_count;i++){

  }
}

//return INVALID_PAGE_ID if it can't find a page
uint32_t FreeSpaceMapPage::GetFreeSpace(){

}

page_id_t FreeSpaceMapPage::GetValidPage(){

}
