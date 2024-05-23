#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"
#include "log_info.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only


/**
 * TODO: Student Implement
 */
struct LogRec {
  LogRec(LogRecType type,txn_id_t txnId):type_(type){
      lsn_ = next_lsn_;
      next_lsn_++;
      if(type == LogRecType::kBegin){
        prev_lsn_map_[txnId]=lsn_;
        prev_lsn_=INVALID_LSN;
      }
      else {
        prev_lsn_=prev_lsn_map_[txnId];
        prev_lsn_map_[txnId]=lsn_;
      }
  };

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    LogInfo* log_info;
    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
inline lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  LogRecPtr logrec(new LogRec(LogRecType::kInsert,txn_id));
  logrec->log_info = new InsertInfo(txn_id,std::move(ins_key),ins_val);
  return logrec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  LogRecPtr logrec(new LogRec(LogRecType::kDelete,txn_id));
  logrec->log_info = new DeleteInfo(txn_id,std::move(del_key),del_val);
  return logrec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  LogRecPtr logrec(new LogRec(LogRecType::kUpdate,txn_id));
  logrec->log_info = new UpdateInfo(txn_id,std::move(old_key),old_val,std::move(new_key),new_val);
  return logrec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  LogRecPtr logrec(new LogRec(LogRecType::kBegin,txn_id));
  logrec->log_info = new BeginInfo(txn_id);
  return logrec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  LogRecPtr logrec(new LogRec(LogRecType::kCommit,txn_id));
  logrec->log_info = new CommitInfo(txn_id);
  return logrec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  LogRecPtr logrec(new LogRec(LogRecType::kAbort,txn_id));
  logrec->log_info = new AbortInfo(txn_id);
  return logrec;
}

#endif  // MINISQL_LOG_REC_H
