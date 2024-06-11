#include "storage/freespace_map.h"

#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(FreeSpaceMapTest, FreeSpaceMapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  page_id_t first_page_id;
  auto first_page = bpm_->NewPage(first_page_id);
  auto freespace_map = new FreeSpaceMap(first_page_id,bpm_);
  bpm_->UnpinPage(first_page_id,true);
  freespace_map->SetNewPair(1,10);
  freespace_map->SetNewPair(2,100);
  freespace_map->SetNewPair(3,1000);

//naive test for the map:

//  auto page_id = freespace_map->GetBegin(5);
//  ASSERT_EQ(page_id,1);
//  page_id = freespace_map->GetNext(5);
//  ASSERT_EQ(page_id,2);
//  page_id = freespace_map->GetNext(5);
//  ASSERT_EQ(page_id,3);
//  page_id = freespace_map->GetBegin(50);
//  ASSERT_EQ(page_id,2);
//  page_id = freespace_map->GetNext(500);
//  ASSERT_EQ(page_id,3);

//random test for the map:

  int pair_num = 10000;
  const int max_page_id = 1000000000;
  const int max_space = 10000;
  vector<pair<page_id_t,uint32_t>>space_map(pair_num);
  unordered_map<page_id_t,int>page_map;
  for(int i=0;i<pair_num;i++) {
    auto rand_page_id = RandomUtils::RandomInt(0, max_page_id);
    while (page_map[rand_page_id]) rand_page_id = RandomUtils::RandomInt(0, max_page_id);
    auto rand_max_space = RandomUtils::RandomInt(0, max_space);
    freespace_map->SetNewPair(rand_page_id, rand_max_space);
    space_map[i]= make_pair(rand_page_id,rand_max_space);
  }
  int need_space = 6000;
  int ans_index=-1;
  auto get_page_id = freespace_map->GetBegin(need_space);
  for(int i=1;i<=10;i++){
    int j;
    for(j=ans_index+1;j<pair_num;j++){
      if(space_map[j].second>=need_space){
        ans_index=j;
        break;
      }
    }
    if(j==pair_num)break;
    ASSERT_EQ(get_page_id,space_map[ans_index].first);
    get_page_id = freespace_map->GetNext(need_space);
  }
//  ASSERT_TRUE(bpm_->CheckAllUnpinned());
  delete freespace_map;
}
