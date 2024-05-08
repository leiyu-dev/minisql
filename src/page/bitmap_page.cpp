#include "page/bitmap_page.h"
#include <iostream>

#include "glog/logging.h"
using std::cout;
using std::endl;

//bitset:set x 1
template <size_t PageSize>
inline void BitmapPage<PageSize>::set(int x) {
    bytes[x>>3]|=(1<<(x&7));
}

//bitset:set x 0
template <size_t PageSize>
inline void BitmapPage<PageSize>::reset(int x) {
  bytes[x>>3]&=(255^(1<<(x&7)));
}

//bitset:get x 0or1
template <size_t PageSize>
inline bool BitmapPage<PageSize>::get(int x) const {
    return (bytes[x>>3]>>(x&7))&1;
}


/**
 *
 * @return true if allocated successfully and false if not
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_+1>MAX_PAGES){
    LOG(WARNING)<<"bitmap is full, can't allocate"<<std::endl;
    return false;
  }
  page_allocated_++;
  set(next_free_page_);
  page_offset=next_free_page_;
  if(page_allocated_==MAX_PAGES){
    next_free_page_= 0;
  }
  else while(!IsPageFree(next_free_page_)){
    next_free_page_++;
    if(next_free_page_>=MAX_PAGES)
      next_free_page_%=MAX_PAGES;
  }
  return true;
}

/**
 *
 * @return true if deallocate successfully
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset>=MAX_PAGES||IsPageFree(page_offset)){
    LOG(WARNING)<<"DeAllocate a unallocated page"<<std::endl;
    return false;
  }
  //todo: reallocate the page
  reset(page_offset);
  if(page_allocated_==MAX_PAGES)next_free_page_=page_offset;
  page_allocated_--;
  return true;
}

/**
 *
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset>=MAX_PAGES)return false;
  return get(page_offset)^1;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if(byte_index*8+bit_index>=MAX_PAGES)return false;
  return (bytes[byte_index]&(1<<bit_index))^1;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;