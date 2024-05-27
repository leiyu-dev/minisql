#pragma once
#include <utility>
#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"
class LogInfo{
 public:
  LogInfo() = default;
  explicit LogInfo(txn_id_t txnId):txn_id(txnId){}
  txn_id_t txn_id{};
  virtual ~LogInfo()= default;
};
using KeyType = std::string;
using ValType = int32_t;

class InsertInfo : public LogInfo{
 public:
  InsertInfo(txn_id_t txnId,KeyType InsKey,ValType InsVal):LogInfo(txnId),ins_key(std::move(InsKey)),ins_val(InsVal){}
  KeyType ins_key;
  ValType ins_val;
};

class DeleteInfo : public LogInfo{
 public:
  DeleteInfo(txn_id_t txnId,KeyType delKey,ValType delVal):LogInfo(txnId),del_key(std::move(delKey)),del_val(delVal){}
  KeyType del_key;
  ValType del_val;
};

class UpdateInfo : public LogInfo{
 public:
  UpdateInfo(txn_id_t txnId,KeyType old_key, ValType old_val, KeyType new_key, ValType new_val):LogInfo(txnId),
     old_key(std::move(old_key)),old_val(old_val),new_key(std::move(new_key)),new_val(new_val){};
  KeyType old_key; ValType old_val; KeyType new_key; ValType new_val;
};

class BeginInfo : public LogInfo{
 public:
  BeginInfo(txn_id_t txnId): LogInfo(txnId){};
};

class CommitInfo : public LogInfo{
 public:
  CommitInfo(txn_id_t txnId): LogInfo(txnId){};
};

class AbortInfo : public LogInfo{
 public:
  AbortInfo(txn_id_t txnId): LogInfo(txnId){};
};

