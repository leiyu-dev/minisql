#include "buffer/lru_replacer.h"
#include "glog/logging.h"
LRUReplacer::LRUReplacer(size_t num_pages):MAX_NUM_PAGES(num_pages),now_size(0),
                                              unpinned(num_pages+5){
  head=new node;
  tail=new node;
  tail->prev=head;
  head->next=tail;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(now_size==0){
    LOG(INFO)<<"victim an empty replacer"<<endl;
    return false;
  }
  auto now=tail->prev;
  (*frame_id)=now->val;
  (now->next)->prev=now->prev;
  (now->prev)->next=now->next;
  map.erase(*frame_id);
  unpinned[*frame_id]=false;
  delete now;
  now_size--;
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(frame_id>MAX_NUM_PAGES){
    LOG(WARNING)<<"the frame_id is out of bound"<<endl;
    return;
  }
  if(!unpinned[frame_id]){
    LOG(WARNING)<<"repin of "<<frame_id<<endl;
    return;
  }
  unpinned[frame_id]=false;
  auto now=map[frame_id];
  (now->next)->prev=now->prev;
  (now->prev)->next=now->next;
  map.erase(frame_id);
  delete now;
  now_size--;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(frame_id>MAX_NUM_PAGES){
    LOG(WARNING)<<"the frame_id is out of bound"<<endl;
    return;
  }
  if(unpinned[frame_id]){//already exists
    LOG(WARNING)<<"reunpin of "<<frame_id<<endl;
    return;
    auto now=map[frame_id];
    (now->next)->prev=now->prev;
    (now->prev)->next=now->next;
    now->prev=head;
    now->next=head->next;
    head->next->prev=now;
    head->next=now;
  }else{
    unpinned[frame_id]=true;
    now_size++;
    auto now=new node;
    now->val=frame_id;
    now->prev=head;
    now->next=head->next;
    head->next->prev=now;
    head->next=now;
    map[frame_id]=now;
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return now_size;
}