#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "common/config.h"
static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
#ifdef ENABLE_BUFFER_DEBUG
  LOG(INFO)<<"fetch page of "<<page_id<<" "<<std::endl;
#endif
  // 1.     Search the page table for the requested page (P).
  if(page_table_.find(page_id)!=page_table_.end()){
      frame_id_t frame_id=page_table_[page_id];
      // 1.1    If P exists, pin it and return it immediately.
      auto page=pages_+frame_id;
      page->Pin();
      replacer_->Pin(frame_id);
      return pages_+frame_id;
  }
  else{
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    frame_id_t frame_id;
    if(!free_list_.empty()){//have free list
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    else if(replacer_->Size()==0){
      LOG(WARNING)<<"all pages in the buffer has been pinned"<<std::endl;
      return nullptr;
    }
    else replacer_ -> Victim(&frame_id);

    auto page=pages_+ frame_id;
    // 2.     If R is dirty, write it back to the disk.
    if(page->IsDirty()){
      disk_manager_->WritePage(page_id,page->GetData());
    }
    page_table_.erase(page->GetPageId());
    // 3.     Delete R from the page table and insert P.
    page->ResetAll();//maybe useless
    page->SetPageId(page_id);
    page_table_[page_id] = frame_id;
    page->Pin();
    replacer_->Pin(frame_id);
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    disk_manager_->ReadPage(page_id,page->GetData());
    return page;
    }
  }



/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  frame_id_t frame_id;
  if(!free_list_.empty()){//have free list
    // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else if(replacer_->Size()==0){
    // 1.   If all the pages in the buffer pool are pinned, return nullptr.
    LOG(WARNING)<<"all pages in the buffer has been pinned"<<std::endl;
    return nullptr;
  }
  else replacer_ -> Victim(&frame_id);
  page_id=AllocatePage();
//  LOG(INFO)<<"allocate a page with logic_id:"<<page_id<<std::endl;
  auto page=pages_+ frame_id;
  if(page->IsDirty()){
    disk_manager_->WritePage(page_id,page->GetData());
  }
  page_table_.erase(page->GetPageId());
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page->ResetMemory();
  page->SetPageId(page_id);
  page_table_[page_id] = frame_id;
  page->Pin();
  replacer_->Pin(frame_id);
  // 4.   Set the page ID output parameter. Return a pointer to P.
#ifdef ENABLE_BUFFER_DEBUG
  LOG(INFO)<<"new "<<page_id<<endl;
#endif
  return page;

}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if(page_table_.find(page_id)!=page_table_.end()){
    DeallocatePage(page_id);
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t frame_id=page_table_[page_id];
  auto page=pages_+frame_id;
  if(page->GetPinCount()!=0){
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  page->ResetAll();
  free_list_.push_front(frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
#ifdef ENABLE_BUFFER_DEBUG
  LOG(INFO)<<"unpin page "<<page_id<<endl;
#endif
  if(page_table_.find(page_id)==page_table_.end()){
    LOG(ERROR)<<"Unpin an unpinned page of "<<page_id<<std::endl;
    return true;
  }
  auto frame_id = page_table_[page_id];
  auto page = pages_+frame_id;
  if(page->pin_count_==0){
#ifdef ENABLE_BUFFER_DEBUG
    LOG(ERROR)<<"[2]Unpin an unpinned page of "<<page_id<<std::endl;
#endif
    return true;
  }
  if(is_dirty)page->SetDirty();
  page->unPin();
  if(page->GetPinCount()==0){
    replacer_->Unpin(frame_id);
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id)==page_table_.end()){
    LOG(INFO)<<"reflush "<<page_id<<std::endl;
    return true;
  }
  auto frame_id=page_table_[page_id];
  auto page=pages_+frame_id;
//  LOG(INFO)<<"flushpage "<<page_id<<" "<<frame_id<<std::endl;
  disk_manager_->WritePage(page_id,page->GetData());
  page->ResetDirty();
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}