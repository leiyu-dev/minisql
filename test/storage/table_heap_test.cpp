#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 50000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  int tot=0;
  for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); iter++) {
    auto row = *iter;
    auto f500 = Field(TypeId::kTypeInt,500);
    if(row.GetField(0)->CompareLessThan(f500)) {
//      cout << row.GetField(0)->toString() << endl;
      tot++;
    }
    else break;
    }
//    std::cout<<row.GetFields().size()<<endl;
    /* do some things */
  ASSERT_EQ(tot,500);

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  pair<int64_t, Fields *> row_v[size+5];
  int cnt=0;

  //random search in the table
  for (auto row_kv : row_values)row_v[++cnt]=row_kv;
  int order[size+5];
  for(int i=1;i<=size;i++)order[i]=i;
  random_shuffle(order+1,order+size+1);
  int tot_size = size;
    for (int i=1;i<=size;i++) {
    auto row_kv = row_v[i];
    tot_size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(tot_size, 0);
}
