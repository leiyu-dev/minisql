#include "buffer/clock_replacer.h"
#include "glog/logging.h"
CLOCKReplacer::CLOCKReplacer(size_t num_pages):capacity(0),MAX_NUM_PAGES(num_pages),
                                             unpinned(num_pages+5){
  now=clock_list.begin();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if(capacity==0){
    LOG(INFO)<<"victim an empty replacer"<<endl;
    return false;
  }
  bool found=false;
  for(size_t i=1;i<=capacity;i++,now++){
    if(now==clock_list.end())now++;
    if(map[*now]==0){
      found=true;
      break;
    }
    else map[*now]=0;
  }
  if(!found){
    for(size_t i=1;i<=capacity;i++,now++){
      if(now==clock_list.end())now++;
      if (map[*now] == 0) {
        found=true;
        break;
      }
    }
  }
  if(!found){
    LOG(ERROR)<<"empty replacer is not detected"<<endl;
    return false;
  }
  auto now_frame_id=*now;
  auto to_be_erased=now;now++;
  (*frame_id)=now_frame_id;
  clock_list.erase(to_be_erased);
  map.erase(now_frame_id);
  unpinned[now_frame_id]=false;
  capacity--;
  return true;
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if(frame_id>MAX_NUM_PAGES){
    LOG(WARNING)<<"the frame_id is out of bound"<<endl;
    return;
  }
  if(!unpinned[frame_id]){
    LOG(WARNING)<<"repin of "<<frame_id<<endl;
    return;
  }
  unpinned[frame_id]=false;
//todo:can't use remove
  for(auto i=clock_list.begin();i!=clock_list.end();i++){
    if(*i==frame_id){
      if(i==now)now++;
      clock_list.erase(i);
      break;
    }
  }
  map.erase(frame_id);
  capacity--;
}

/**
 * TODO: Student Implement
 */
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if(frame_id>MAX_NUM_PAGES){
    LOG(WARNING)<<"the frame_id is out of bound"<<endl;
    return;
  }
  if(unpinned[frame_id]){//already exists
    LOG(WARNING)<<"reunpin of "<<frame_id<<endl;
    return;
  }else{
    unpinned[frame_id]=true;
    capacity++;
    if(now==clock_list.end()){
      now++;
    }
    clock_list.insert(now,frame_id);
    map[frame_id]=1;
  }
}

/**
 * TODO: Student Implement
 */
size_t CLOCKReplacer::Size() {
  return capacity;
}