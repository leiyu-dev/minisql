#include "storage/freespace_map.h"

#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "freespace_map_test.db";
using Fields = std::vector<Field>;

TEST(FreeSpaceMapTest, FreeSpaceMapSampleTest) {
  // init testing instance
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  page_id_t first_page_id;
  auto first_page = bpm_->NewPage(first_page_id);
  auto freespace_map = new FreeSpaceMap(first_page_id,bpm_);
  bpm_->UnpinPage(first_page_id,true);


//naive test for the map:
  page_id_t page_id;
  freespace_map->SetNewPair(1,10);
  page_id = freespace_map->GetBegin(50);
  ASSERT_EQ(page_id,1);
  page_id = freespace_map->GetNext(50);
  ASSERT_EQ(page_id,INVALID_PAGE_ID);
  freespace_map->SetNewPair(2,100);
  freespace_map->SetNewPair(3,1000);
  page_id = freespace_map->GetBegin(5);
  ASSERT_EQ(page_id,1);
  page_id = freespace_map->GetNext(5);
  ASSERT_EQ(page_id,2);
  page_id = freespace_map->GetNext(5);
  ASSERT_EQ(page_id,3);
  page_id = freespace_map->GetBegin(50);
  ASSERT_EQ(page_id,2);
  page_id = freespace_map->GetNext(500);
  ASSERT_EQ(page_id,3);
  page_id = freespace_map->GetBegin(5000);
  ASSERT_EQ(page_id,3);
  page_id = freespace_map->GetNext(5000);
  ASSERT_EQ(page_id,INVALID_PAGE_ID);
  ASSERT_TRUE(bpm_->CheckAllUnpinned());
  return;
//random test for the map:

  int pair_num = 100000;
  const int max_page_id = 1000000000;
  const int max_space = 10000;
  vector<pair<page_id_t,uint32_t>>space_map(pair_num);
  unordered_map<page_id_t,int>page_map;
  for(int i=0;i<pair_num;i++) {
//    auto rand_page_id = RandomUtils::RandomInt(0, max_page_id);
//    while (page_map[rand_page_id]) rand_page_id = RandomUtils::RandomInt(0, max_page_id);
    auto rand_page_id = i;
    auto rand_max_space = RandomUtils::RandomInt(0, max_space);
    freespace_map->SetNewPair(rand_page_id, rand_max_space);
    space_map[i]= make_pair(rand_page_id,rand_max_space);
  }

  //search test
  uint32_t need_space = 6000;
  int ans_index=-1;
  auto get_page_id = freespace_map->GetBegin(need_space);
  for(int i=1;i<=10000;i++){
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


  //change all space and test
  for(int i=0;i<pair_num;i++) {
    auto rand_max_space = RandomUtils::RandomInt(0, max_space);
    ASSERT_NE(INVALID_PAGE_ID,freespace_map->SetFreeSpace(space_map[i].first, rand_max_space));
    space_map[i].second = rand_max_space;
  }
  ans_index=-1;
  get_page_id = freespace_map->GetBegin(need_space);
  for(int i=1;i<=10000;i++){
    int j;
    for(j=ans_index+1;j<pair_num;j++){
      if(space_map[j].second>=need_space){
        ans_index=j;
        break;
      }
    }
    if(j==pair_num)break;
    if(get_page_id!=space_map[ans_index].first){
      cout<<"OHGHH!"<<freespace_map->GetFreeSpace(freespace_map->page_index,freespace_map->internal_index)<<endl<<
          freespace_map->page_index*freespace_map->MAX_PAIR_COUNT + freespace_map->internal_index+1<<endl<<
          j<<endl;
      ASSERT_EQ(get_page_id,space_map[ans_index].first);
    }
    get_page_id = freespace_map->GetNext(need_space);
  }


  ASSERT_TRUE(bpm_->CheckAllUnpinned());
  delete freespace_map;
}
