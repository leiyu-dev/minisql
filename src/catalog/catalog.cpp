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
      for(auto [table_id,page_id]:catalog_meta_->table_meta_pages_){
          TableMetadata* table_meta;
          auto table_page = buffer_pool_manager->FetchPage(page_id);
          TableMetadata::DeserializeFrom(table_page->GetData(),table_meta);//new TableMetadata in it
          auto table_heap =  TableHeap::Create(buffer_pool_manager,table_meta->GetFirstPageId(),table_meta->GetSchema(),
                                              log_manager,lock_manager);
          auto table_info = TableInfo::Create();
          table_info->Init(table_meta,table_heap);
          tables_[table_id] = table_info;
          table_names_[table_meta->GetTableName()]=table_id;
      }
      for(auto [index_id,page_id]:catalog_meta_->index_meta_pages_){//[index_id,page_id]
          IndexMetadata* index_meta;
          auto index_page = buffer_pool_manager->FetchPage(page_id);
          IndexMetadata::DeserializeFrom(index_page->GetData(),index_meta);
          auto table_info = tables_[index_meta->GetTableId()];
          auto index_info = IndexInfo::Create();
          index_info->Init(index_meta,table_info,buffer_pool_manager);
          indexes_[index_id]=index_info;
          index_names_[table_info->GetTableName()][index_meta->GetIndexName()]=index_id;
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

  auto deep_copied_schema = Schema::DeepCopySchema(schema);
//  initialize table info
  auto table_heap = TableHeap::Create(buffer_pool_manager_,deep_copied_schema,txn,log_manager_,lock_manager_);
  auto table_meta = TableMetadata::Create(table_id,table_name,table_heap->GetFirstPageId(),deep_copied_schema);
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
  auto table_id = table_names_[table_name];
  if(tables_.find(table_id)==tables_.end()){
    LOG(INFO)<<"Unknown Error When finding table_info"<<endl;
    return DB_FAILED;
  }
  table_info=tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(const auto& [table_name,table_id] : table_names_){
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

//  if(index_names_.find(table_name)==index_names_.end()){
//    LOG(ERROR)<<"Inconsistent of table_names_ and index_names_"<<endl;
//    exit(0);
//    return DB_FAILED;
//  }

  auto& index_map = index_names_[table_name];

//  auto& index_map = index_names_.find(table_name)->second;
//  if(index_map==index_map2)cout<<"ok"<<endl;
  //solved :: unknown error : index_map = index_names_.find(table_name).second
  //index_names_ is not consistent with index_names_ when index in a table is empty

  if(index_map.find(index_name)!=index_map.end()){
    LOG(INFO)<<"Duplicated index name"<<endl;
    return DB_INDEX_ALREADY_EXIST;
  }
  if(index_type!="bptree"){
    LOG(ERROR)<<"Unsupported index type"<<endl;
    return DB_FAILED;
  }
  //create key_map
  auto table_id = table_names_[table_name];
  if(tables_.find(table_id)==tables_.end()){
    LOG(INFO)<<"Unknown Error When finding table_info"<<endl;
    return DB_FAILED;
  }
  auto table_info = tables_[table_id];
  vector<uint32_t>key_map;
  for(auto i : index_keys){
      auto column = table_info->GetSchema()->GetColumns();
      bool find_column=false;
      for(auto j : column){
        if(j->GetName()==i){
          key_map.emplace_back(j->GetTableInd());
          find_column=true;
          break;
        }
      }
      if(!find_column){
        LOG(INFO)<<"bad column name"<<endl;
        return DB_COLUMN_NAME_NOT_EXIST;
      }
  }

  //create id
  auto index_id = catalog_meta_->GetNextIndexId();
  index_names_[table_name][index_name]=index_id;

  //init info and map it
  auto index_meta = IndexMetadata::Create(index_id,index_name,table_id,key_map);
  index_info = IndexInfo::Create();
  index_info->Init(index_meta,table_info,buffer_pool_manager_);
  indexes_[index_id]=index_info;

  //create page
  page_id_t page_id;
  auto index_meta_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_[index_id]=page_id;
  index_meta->SerializeTo(index_meta_page->GetData());

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name)==table_names_.end()){
    LOG(INFO)<<"Cannot find table"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_.find(table_name)->second.find(index_name)==index_names_.find(table_name)->second.end()){
    LOG(INFO)<<"Cannot find index"<<endl;
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names_.find(table_name)->second.find(index_name)->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) {
  if(table_names_.find(table_name)==table_names_.end()){
    LOG(INFO)<<"Cannot find table"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  //the function is not defined const to deal with the nullptr of index_map
  auto& index_map = index_names_[table_name];
  
  for(auto [index_name,index_id] : index_map){
    if(indexes_.find(index_id)==indexes_.end()){
      LOG(ERROR)<<"Known error when finding index info"<<endl;
      return DB_FAILED;
    }
    indexes.emplace_back(indexes_.find(index_id)->second);
  }
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  #ifdef ENABLE_CATALOG_DEBUG
  LOG(INFO)<<"drop table "<<table_name<<endl;
  #endif
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
  if(table_names_.find(table_name)==table_names_.end()){
    LOG(INFO)<<"Try to drop a not existed table"<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_.find(table_name)->second.find(index_name)==index_names_.find(table_name)->second.end()){
    LOG(INFO)<<"Cannot find index"<<endl;
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names_.find(table_name)->second.find(index_name)->second;
  if(indexes_.find(index_id)==indexes_.end()){
    LOG(ERROR)<<"Unknown error when finding index info"<<endl;
    return DB_FAILED;
  }
  auto index_info = indexes_[index_id];
  delete index_info;
  indexes_.erase(index_id);
  index_names_.find(table_name)->second.erase(index_name);
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropAllIndexes(const string &index_name,uint32_t& drop_tot) {//drop indexes in all table
  for(auto &[table_name,table_id] : table_names_){
    if(index_names_.find(table_name)->second.find(index_name)==index_names_.find(table_name)->second.end()){
      continue;
    }
    auto index_id = index_names_.find(table_name)->second.find(index_name)->second;
    if(indexes_.find(index_id)==indexes_.end()){
      LOG(ERROR)<<"Unknown error when finding index info"<<endl;
      return DB_FAILED;
    }
    auto index_info = indexes_[index_id];
    delete index_info;
    indexes_.erase(index_id);
    index_names_.find(table_name)->second.erase(index_name);
    drop_tot++;
    // ASSERT(false, "Not Implemented yet"); 
  }
  return DB_SUCCESS;
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
  if(tables_.find(table_id)!=tables_.end()){
    LOG(INFO)<<"load a existed table"<<endl;
    return DB_TABLE_ALREADY_EXIST;
  }
  TableMetadata* table_meta;
  auto table_page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata::DeserializeFrom(table_page->GetData(),table_meta);//new TableMetadata in it
  auto table_heap =  TableHeap::Create(buffer_pool_manager_,table_meta->GetFirstPageId(),table_meta->GetSchema(),
                                      log_manager_,lock_manager_);
  auto table_info = TableInfo::Create();
  table_info->Init(table_meta,table_heap);
  tables_[table_id] = table_info;
  table_names_[table_meta->GetTableName()]=table_id;

  catalog_meta_->table_meta_pages_[table_id]=page_id;
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  IndexMetadata* index_meta;
  auto index_page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata::DeserializeFrom(index_page->GetData(),index_meta);
  auto table_info = tables_[index_meta->GetTableId()];
  auto index_info = IndexInfo::Create();
  index_info->Init(index_meta,table_info,buffer_pool_manager_);
  indexes_[index_id]=index_info;
  index_names_[table_info->GetTableName()][index_meta->GetIndexName()]=index_id;

  catalog_meta_->index_meta_pages_[index_id]=page_id;
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {

  if(tables_.find(table_id)==tables_.end()){
    LOG(INFO)<<"Unknown Error When finding table_info"<<endl;
    return DB_FAILED;
  }
  table_info=tables_[table_id];
  return DB_SUCCESS;
}