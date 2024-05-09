#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
//  for(size_t i=0;i<MAX_BITMAP;i++){
//        char *bitmapPage_meta = new char[PAGE_SIZE];
//        memset(bitmapPage_meta,0,PAGE_SIZE*sizeof(char));
//        WritePhysicalPage(i*(BITMAP_SIZE+1)+1,bitmapPage_meta);
//  }
  LOG(INFO)<<"finish initialization"<<std::endl;
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  auto* metaPage = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  page_id_t i=0;
  for(i=0;i<metaPage->GetExtentNums();i++){
    if(metaPage->GetExtentUsedPage(i)<BITMAP_SIZE){
      break;
    }
  }
  if(i==metaPage->GetExtentNums()) {  // don't have available bitmap pages!
    if (metaPage->num_extents_ == MAX_BITMAP) {
        LOG(WARNING)<<"meta page is full!!"<<std::endl;
        return INVALID_PAGE_ID;
    }
    metaPage->num_extents_++;
  }
  char *bitmapPage_meta = new char[PAGE_SIZE];
  ReadPhysicalPage(i*(BITMAP_SIZE+1)+1,bitmapPage_meta);
  LOG(INFO)<<"read the bitmap of "<<i*(BITMAP_SIZE+1)+1<<std::endl;
  auto* bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmapPage_meta);
  uint32_t inner_index;
  metaPage->extent_used_page_[i]++;
  metaPage->num_allocated_pages_++;
  bitmapPage->AllocatePage(inner_index);
  WritePhysicalPage(i*(BITMAP_SIZE+1)+1,bitmapPage_meta);
  delete[] bitmapPage_meta;//todo:maybe don't need to frequent I/O of bitmap_page
  LOG(INFO)<<"return a page,logical_id with "<<i<<' '<<BITMAP_SIZE<<' '<<inner_index<<std::endl;
  return i*BITMAP_SIZE+inner_index;//logical id
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto* metaPage = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  if(logical_page_id>=MAX_VALID_PAGE_ID){
    LOG(WARNING)<<"invalid logical_id: "<<logical_page_id<<std::endl;
    return;
  }
  size_t bitmap_id=(logical_page_id+BITMAP_SIZE)/BITMAP_SIZE-1;
  size_t inner_index=logical_page_id%BITMAP_SIZE;
  char* bitmapPage_meta = new char[PAGE_SIZE];;
  ReadPhysicalPage(bitmap_id*(BITMAP_SIZE+1)+1,bitmapPage_meta);
  auto* bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmapPage_meta);
  metaPage->extent_used_page_[bitmap_id]--;
  metaPage->num_allocated_pages_--;
  bitmapPage->DeAllocatePage(inner_index);
  WritePhysicalPage(bitmap_id*(BITMAP_SIZE+1)+1,bitmapPage_meta);
  delete[] bitmapPage_meta;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if(logical_page_id>=MAX_VALID_PAGE_ID){
    LOG(WARNING)<<"invalid logical_id"<<std::endl;
    return false;
  }
  page_id_t bitmap_id=(logical_page_id+PAGE_SIZE)/PAGE_SIZE-1;
  page_id_t inner_index=logical_page_id%PAGE_SIZE;
  char* bitmapPage_meta = new char[PAGE_SIZE];
  ReadPhysicalPage(bitmap_id*(BITMAP_SIZE+1)+1,bitmapPage_meta);
  auto* bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmapPage_meta);
  delete[] bitmapPage_meta;
  return bitmapPage->IsPageFree(inner_index);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t bitmap_id=logical_page_id/PAGE_SIZE;
  page_id_t inner_index=logical_page_id%PAGE_SIZE;
  return bitmap_id*(BITMAP_SIZE+1)+inner_index+2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  LOG(INFO)<<"write "<<physical_page_id<<std::endl;
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}