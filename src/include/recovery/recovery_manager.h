#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>
#include "glog/logging.h"
#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;
using Unended_Txn = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  /**
   * TODO: Student Implement
   */
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = last_checkpoint.active_txns_;
    data_ = last_checkpoint.persist_data_;
  }

  /**
   * TODO: Student Implement
   * 先进行重做，对已经commit or abort的 事务重做
   */
  void RedoPhase() {
    unendedTxn = active_txns_ ;
    auto now = log_recs_.find(persist_lsn_);
    now++;
    while (now != log_recs_.end()) {
      auto log_rec = now->second;

      //add new txns
      unendedTxn[log_rec->log_info->txn_id]=log_rec->lsn_;

      if(log_rec->type_==LogRecType::kCommit){
        unendedTxn.erase(log_rec->log_info->txn_id);//end
      }

      if(log_rec->type_==LogRecType::kAbort){
        UndoTxn(log_rec->prev_lsn_);
        unendedTxn.erase(log_rec->log_info->txn_id);//end
      }

      if (log_rec->type_ == LogRecType::kUpdate) {
        auto log_info = dynamic_cast<UpdateInfo *>(log_rec->log_info);

        data_.erase(log_info->old_key);
        data_.emplace(log_info->new_key, log_info->new_val);

      }
      if (log_rec->type_ == LogRecType::kDelete) {
        auto log_info = dynamic_cast<DeleteInfo *>(log_rec->log_info);

        if (data_[log_info->del_key] != log_info->del_val) {
          LOG(ERROR) << "Inconsistent of data here" << std::endl;
          now++;continue;
        }
        data_.erase(log_info->del_key);

      }
      if (log_rec->type_ == LogRecType::kInsert) {
        auto log_info = dynamic_cast<InsertInfo *>(log_rec->log_info);

        if (data_.find(log_info->ins_key) != data_.end()) {
          LOG(ERROR) << "Inconsistent of data here" << std::endl;
          exit(1);
        }
        data_.emplace(log_info->ins_key, log_info->ins_val);

      }
      if (log_rec->type_ == LogRecType::kInvalid) {
        LOG(ERROR) << "Get Invalid Id when recover" << std::endl;
        exit(1);
      }
      now++;
    }
  }

  /**
   * TODO: Student Implement
   * 针对没有结束的事务倒着做回滚
   */
  void UndoPhase() {

    for(auto [txn_id,last_lsn] : unendedTxn){
      UndoTxn(last_lsn);
    }
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) {
    log_recs_.emplace(log_rec->lsn_, log_rec);
    //I don't know where to update the data_,maybe here?
    //   - No Need to Update!
//    if(log_rec->type_==LogRecType::kCommit) {
//      while(log_recs_[log_rec->prev_lsn_]->type_!=LogRecType::kBegin) {
//        log_rec=log_recs_[log_rec->prev_lsn_];
//        if (log_rec->type_ == LogRecType::kInsert) {
//          auto log_info = dynamic_cast<InsertInfo *>(log_rec->log_info);
//          data_[log_info->ins_key] = log_info->ins_val;
//          LOG(INFO)<<"insert"<<log_info->ins_key<<" "<<log_info->ins_val<<std::endl;
//        }
//        if (log_rec->type_ == LogRecType::kDelete) {
//          auto log_info = dynamic_cast<DeleteInfo *>(log_rec->log_info);
//          data_.erase(log_info->del_key);
//          LOG(INFO)<<"delete"<<log_info->del_key<<" "<<log_info->del_val<<std::endl;
//        }
//        if (log_rec->type_ == LogRecType::kUpdate) {
//          auto log_info = dynamic_cast<UpdateInfo *>(log_rec->log_info);
//          data_.erase(log_info->old_key);
//          data_.emplace(log_info->new_key, log_info->new_val);
//          LOG(INFO)<<"replace["<<log_info->old_key<<","<<log_info->old_val<<"]->["<<log_info->new_key<<","<<log_info->new_val<<"]"<<std::endl;
//        }
//      }
//    }

  }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:

  void UndoTxn(txn_id_t last_lsn){
    auto log_rec = log_recs_[last_lsn];
    while(log_rec->type_!=LogRecType::kBegin) {

      if (log_rec->type_ == LogRecType::kBegin){
        LOG(ERROR)<<"Inconsistent of records here"<<std::endl;
        log_rec = log_recs_[log_rec->prev_lsn_];continue;
      }
      if (log_rec->type_ == LogRecType::kAbort){
        LOG(ERROR)<<"Inconsistent of records here"<<std::endl;
        log_rec = log_recs_[log_rec->prev_lsn_];continue;
      }
      if (log_rec->type_ == LogRecType::kCommit){
        LOG(ERROR)<<"Inconsistent of records here"<<std::endl;
        log_rec = log_recs_[log_rec->prev_lsn_];continue;
      }

      if (log_rec->type_ == LogRecType::kUpdate) {
        auto log_info = dynamic_cast<UpdateInfo *>(log_rec->log_info);
        data_.erase(log_info->new_key);
        data_.emplace(log_info->old_key, log_info->old_val);
      }
      if (log_rec->type_ == LogRecType::kDelete) {
        auto log_info = dynamic_cast<DeleteInfo *>(log_rec->log_info);
        if (data_.find(log_info->del_key) != data_.end()) {
          LOG(ERROR) << "Inconsistent of data here" << std::endl;
          log_rec = log_recs_[log_rec->prev_lsn_];continue;
        }
        data_.emplace(log_info->del_key, log_info->del_val);
      }

      if (log_rec->type_ == LogRecType::kInsert) {
        auto log_info = dynamic_cast<InsertInfo *>(log_rec->log_info);
        if (data_.find(log_info->ins_key) == data_.end()) {
          LOG(ERROR) << "Inconsistent of data here" << std::endl;
          log_rec = log_recs_[log_rec->prev_lsn_];continue;
        }
        data_.erase(log_info->ins_key);
      }

      if (log_rec->type_ == LogRecType::kInvalid) {
        LOG(ERROR) << "Get Invalid Id when recover" << std::endl;
        log_rec = log_recs_[log_rec->prev_lsn_];continue;
      }

      log_rec = log_recs_[log_rec->prev_lsn_];
    }
  }


  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  Unended_Txn unendedTxn{};
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
