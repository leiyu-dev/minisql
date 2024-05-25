#ifndef MINISQL_TABLE_HEAP_H
#define MINISQL_TABLE_HEAP_H

#include "buffer/buffer_pool_manager.h"
#include "concurrency/lock_manager.h"
#include "page/header_page.h"
#include "page/table_page.h"
#include "recovery/log_manager.h"
#include "storage/table_iterator.h"

class TableHeap {
  friend class TableIterator;

 public:
  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, Schema *schema, Txn *txn, LogManager *log_manager,
                           LockManager *lock_manager) {
    return new TableHeap(buffer_pool_manager, schema, txn, log_manager, lock_manager);
  }

  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                           LogManager *log_manager, LockManager *lock_manager) {
    return new TableHeap(buffer_pool_manager, first_page_id, schema, log_manager, lock_manager);
  }

    // 获取 TablePage
    TablePage* FetchPage(page_id_t page_id) const {
        return reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    }

    // 取消页面的固定
    void UnpinPage(page_id_t page_id, bool is_dirty) const {
        buffer_pool_manager_->UnpinPage(page_id, is_dirty);
    }

  ~TableHeap() {}

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The recovery performing the insert
   * @return true iff the insert is successful
   */
  bool InsertTuple(Row &row, Txn *txn);

  /**
   * Mark the tuple as deleted. The actual delete will occur when ApplyDelete is called.
   * @param[in] rid Resource id of the tuple of delete
   * @param[in] txn Txn performing the delete
   * @return true iff the delete is successful (i.e the tuple exists)
   */
  bool MarkDelete(const RowId &rid, Txn *txn);

  /**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Txn performing the update
   * @return true is update is successful.
   */
  bool UpdateTuple(Row &row, const RowId &rid, Txn *txn);

  /**
   * Called on Commit/Abort to actually delete a tuple or rollback an insert.
   * @param rid Rid of the tuple to delete
   * @param txn Txn performing the delete.
   */
  void ApplyDelete(const RowId &rid, Txn *txn);

  /**
   * Called on abort to rollback a delete.
   * @param[in] rid Rid of the deleted tuple.
   * @param[in] txn Txn performing the rollback
   */
  void RollbackDelete(const RowId &rid, Txn *txn);

  /**
   * Read a tuple from the table.
   * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
   * @param[in] txn recovery performing the read
   * @return true if the read was successful (i.e. the tuple exists)
   */
  bool GetTuple(Row *row, Txn *txn);

  void FreeTableHeap() {
    auto next_page_id = first_page_id_;
    while (next_page_id != INVALID_PAGE_ID) {
      auto old_page_id = next_page_id;
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(old_page_id));
      assert(page != nullptr);
      next_page_id = page->GetNextPageId();
      buffer_pool_manager_->UnpinPage(old_page_id, false);
      buffer_pool_manager_->DeletePage(old_page_id);
    }
  }

  /**
   * Free table heap and release storage in disk file
   */
  void DeleteTable(page_id_t page_id = INVALID_PAGE_ID);

  /**
   * @return the begin iterator of this table
   */
  TableIterator Begin(Txn *txn);

  /**
   * @return the end iterator of this table
   */
  TableIterator End();

  /**
   * @return the id of the first page of this table
   */
  inline page_id_t GetFirstPageId() const { return first_page_id_; }

 private:
  /**
   * create table heap and initialize first page
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, Schema *schema, Txn *txn, LogManager *log_manager,
                     LockManager *lock_manager)
      : buffer_pool_manager_(buffer_pool_manager),
        schema_(schema),
        log_manager_(log_manager),
        lock_manager_(lock_manager) {
//    ASSERT(false, "Not implemented yet.");
      TablePage* true_page = reinterpret_cast<TablePage*>(buffer_pool_manager->NewPage(first_page_id_));//初始化新获得数据页
      true_page->Init(first_page_id_ ,INVALID_PAGE_ID,log_manager_, nullptr);
      buffer_pool_manager->UnpinPage(first_page_id_,true);
  };

  explicit TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                     LogManager *log_manager, LockManager *lock_manager)
      : buffer_pool_manager_(buffer_pool_manager),
        first_page_id_(first_page_id),
        schema_(schema),
        log_manager_(log_manager),
        lock_manager_(lock_manager) {}

 private:
  BufferPoolManager *buffer_pool_manager_;
  page_id_t first_page_id_;
  Schema *schema_;
  [[maybe_unused]] LogManager *log_manager_;
  [[maybe_unused]] LockManager *lock_manager_;
};

#endif  // MINISQL_TABLE_HEAP_H
