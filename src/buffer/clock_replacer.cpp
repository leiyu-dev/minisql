#include "buffer/clock_replacer.h"
#include "glog/logging.h"
CLOCKReplacer::CLOCKReplacer(size_t num_pages):capacity(0),MAX_NUM_PAGES(num_pages),
                                             unpinned(num_pages+5){
  now=clock_list.begin();
  status.resize(MAX_NUM_PAGES+5);
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  if(capacity==0){
    LOG(INFO)<<"victim an empty replacer"<<endl;
    return false;
  }
  bool found=false;
  //find the first place that will victim
  for(size_t i=1;i<=capacity;i++,now++){
    if(now==clock_list.end())now++;//loop
    if(status[*now]==0){
      found=true;
      break;
    }
    else
      status[*now]=0;
  }


  if(!found){//at most two loop
    for(size_t i=1;i<=capacity;i++,now++){
      if(now==clock_list.end())now++;
      if (status[*now] == 0) {
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
//  status.erase(now_frame_id);
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
#ifdef ENABLE_BUFFER_DEBUG
    LOG(WARNING)<<"repin of "<<frame_id<<endl;
#endif
    return;
  }
  unpinned[frame_id]=false;
//finished:can't use remove
  if(map.find(frame_id)==map.end()){
    LOG(ERROR)<<"Invalid Pin"<<endl;
    return;
  }
  auto to_be_released = map[frame_id];
  if(*to_be_released==*now)now++;
  clock_list.erase(to_be_released);
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
    if(now==clock_list.end()){//loop
      now++;
    }
    clock_list.insert(now,frame_id);
    now--;
    map[frame_id]=now;
    now++;
    status[frame_id]=1;
  }
}

/**
 * TODO: Student Implement
 */
size_t CLOCKReplacer::Size() {
  return capacity;
}