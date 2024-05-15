#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t ofs=0;
//  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  ofs += 4;
//  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  ofs += 4;
//  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  ofs += 4;
  for (auto iter : table_meta_pages_) {
//    MACH_WRITE_TO(table_id_t, buf, iter.first);
    ofs += 4;
//    MACH_WRITE_TO(page_id_t, buf, iter.second);
    ofs += 4;
  }
  for (auto iter : index_meta_pages_) {
//    MACH_WRITE_TO(index_id_t, buf, iter.first);
    ofs += 4;
//    MACH_WRITE_TO(page_id_t, buf, iter.second);
    ofs += 4;
  }
  return ofs;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if(init){
      catalog_meta_=CatalogMeta::NewInstance();
  }
  else{
      auto catalog_meta_page=buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
      catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData());
      for(auto i:catalog_meta_->table_meta_pages_){//[table_id,page_id]
          TableMetadata* table_meta;
          auto table_page = buffer_pool_manager->FetchPage(i.second);
          TableMetadata::DeserializeFrom(table_page->GetData(),table_meta);//new TableMetadata in it
          auto table_heap =  TableHeap::Create(buffer_pool_manager,table_meta->GetFirstPageId(),table_meta->GetSchema(),
                                              log_manager,lock_manager);
          auto table_info = TableInfo::Create();
          table_info->Init(table_meta,table_heap);
          tables_[i.first] = table_info;
          table_names_[table_meta->GetTableName()]=i.first;
      }
      for(auto i:catalog_meta_->index_meta_pages_){//[index_id,page_id]
          IndexMetadata* index_meta;
          auto index_page = buffer_pool_manager->FetchPage(i.second);
          IndexMetadata::DeserializeFrom(index_page->GetData(),index_meta);
          auto table_info = tables_[index_meta->GetTableId()];
          auto index_info = IndexInfo::Create();
          index_info->Init(index_meta,table_info,buffer_pool_manager);
          indexes_[i.first]=index_info;
          index_names_[table_info->GetTableName()][index_meta->GetIndexName()]=i.first;
      }
  }
//    ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if(table_names_.find(table_name)!=table_names_.end()){
    LOG(INFO)<<"Duplicated table name"<<endl;
    return DB_TABLE_ALREADY_EXIST;
  }
  auto table_id = catalog_meta_->GetNextTableId();
  table_names_[table_name]=table_id;//get id

//  initialize table info
  auto table_heap = TableHeap::Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_);
  auto table_meta = TableMetadata::Create(table_id,table_name,table_heap->GetFirstPageId(),schema);
  table_info = TableInfo::Create();
  table_info->Init(table_meta,table_heap);
  tables_[table_id] = table_info;

  //get a new page for table_meta
  page_id_t page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_[table_id]=page_id;

  //write it into disk
  table_meta->SerializeTo(table_meta_page->GetData());
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name)==table_names_.end()){
    LOG(INFO)<<"Cannot find table"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  if(tables_.find(table_names_[table_name])==tables_.end()){
    LOG(INFO)<<"Unknown Error When finding table_info"<<endl;
    return DB_FAILED;
  }
  table_info=tables_[table_names_[table_name]];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(const auto& i : table_names_){
    table_id_t table_id=i.second;
    if(tables_.find(table_id)==tables_.end()){
      LOG(INFO)<<"Unknown Error When finding table_info"<<endl;
      return DB_FAILED;
    }
    tables.emplace_back(tables_.find(table_id)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if(table_names_.find(table_name)==table_names_.end()){
    LOG(INFO)<<"Cannot find table"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_.find(table_name)->second.find(index_name)!=index_names_.find(table_name)->second.end()){
    LOG(INFO)<<"Duplicated index name"<<endl;
    return DB_INDEX_ALREADY_EXIST;
  }
  if(index_type!="bptree"){
    LOG(ERROR)<<"Unsupported index type"<<endl;
    return DB_FAILED;
  }
  //create id

  //create page


  auto index_meta = IndexMetadata::Create();
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name)==table_names_.end()){
    LOG(INFO)<<"Try to drop a not existed table"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  auto table_id = table_names_[table_name];
  auto table_info = tables_[table_id];
  delete table_info;
  table_names_.erase(table_name);
  tables_.erase(table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  if(!buffer_pool_manager_->FlushPage(META_PAGE_ID)){
    LOG(WARNING)<<"Flush failed"<<endl;
    return DB_FAILED;
  }
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}