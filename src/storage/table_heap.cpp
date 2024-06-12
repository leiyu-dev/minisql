#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    // 获取row对应的TablePage最大支持大小
    size_t max_size_row = TablePage::SIZE_MAX_ROW;
    auto need_space = row.GetSerializedSize(schema_);
    // 如果单行数据超过此大小，返回false
    if (need_space > max_size_row) {
        return false;
    }

    // 从Table中第一逻辑页开始遍历获得free page用于填入该row
#ifdef USE_FREESPACE_MAP
    page_id_t next_page_id = freespace_map_->GetBegin(need_space);
#else
    page_id_t next_page_id = GetFirstPageId();
#endif
    TablePage* true_page = nullptr;

    while (true) {
        // 无效页ID处理
        if (next_page_id == INVALID_PAGE_ID) {
            // 分配新页
            TablePage* new_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(next_page_id));
            if (new_page == nullptr) {
                return false;
            }
            // 初始化新页
            new_page->Init(next_page_id, true_page ? true_page->GetPageId() : INVALID_PAGE_ID, log_manager_, txn);
            new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
#ifdef USE_FREESPACE_MAP
            freespace_map_->SetNewPair(next_page_id,new_page->GetFreeSpace());
#endif
            buffer_pool_manager_->UnpinPage(next_page_id, true);

            // 更新原数据页的next_page_id并unpin写回
            if (true_page) {
                true_page->SetNextPageId(next_page_id);
                buffer_pool_manager_->UnpinPage(true_page->GetPageId(), true);
            }
            return true;
        }

        // 获取当前页
        true_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(next_page_id));
        if (true_page == nullptr) {
            return false;
        }

        // 尝试在当前页插入row
        if (true_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
#ifdef USE_FREESPACE_MAP
          freespace_map_->SetFreeSpace(next_page_id,true_page->GetFreeSpace());
#endif
            buffer_pool_manager_->UnpinPage(next_page_id, true);
            return true;
        }

        // 当前页空间不足，移动到下一页
        buffer_pool_manager_->UnpinPage(next_page_id, false);
#ifdef USE_FREESPACE_MAP
        next_page_id = freespace_map_->GetNext(need_space);
#else
        next_page_id = true_page->GetNextPageId();
#endif
    }
}



bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the recovery.
    if (page == nullptr) {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
#ifdef USE_FREESPACE_MAP
    freespace_map_->SetFreeSpace(page->GetPageId(),page->GetFreeSpace());
#endif
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    // 检查rid是否非法
    if (rid == INVALID_ROWID) {
        return false;
    }

    // 获取原数据对应的数据页
    TablePage* true_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (true_page == nullptr) {
        return false;
    }

    // 通过rowid唯一标识建立row对象
    Row ori_row = Row(rid);

    // 获取原始元组
    bool get_tuple_result = GetTuple(&ori_row, txn);
    if (!get_tuple_result) {
        buffer_pool_manager_->UnpinPage(true_page->GetTablePageId(), false);
        return false;
    }

    // 更新数据页中的元组
    true_page->WLatch();
    int update_tuple_result = true_page->UpdateTuple(row, &ori_row, schema_, txn, lock_manager_, log_manager_);
    true_page->WUnlatch();

    // 根据更新结果进行处理
#ifdef USE_FREESPACE_MAP
    freespace_map_->SetFreeSpace(true_page->GetPageId(), true_page->GetFreeSpace());
#endif
    buffer_pool_manager_->UnpinPage(true_page->GetTablePageId(), update_tuple_result == 0);

    switch (update_tuple_result) {
        case 0: // 更新成功
            return true;
        case -1: // 元组已被删除或slot_num越界
        case 1:  // 其他错误
            return false;
        case 2: // 因为位置不足而更新失败
            // 可以添加特殊处理逻辑
            return false;
        default: // 处理其他未预见的返回值
            return false;
    }
}



/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page != nullptr) {
        page->WLatch(); // 获取写锁
        page->ApplyDelete(rid, txn, log_manager_);
        page->WUnlatch(); // 释放写锁
#ifdef USE_FREESPACE_MAP
        freespace_map_->SetFreeSpace(rid.GetPageId(), page->GetFreeSpace());
#endif
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    }
}


void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Rollback to delete.
    page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
    page->WUnlatch();
#ifdef USE_FREESPACE_MAP
    freespace_map_->SetFreeSpace(rid.GetPageId(), page->GetFreeSpace());
#endif
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    //借助row对象获得对映数据页
    TablePage* true_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    //若数据页不存在，直接返回false
    if(true_page == nullptr)
        return false;
    //true_page->RLatch();
    //读取对映数据并返回判断结果
    bool gettuple_result = true_page->GetTuple(row,schema_,txn,lock_manager_);
    //true_page->RUnlatch();
    //unpin对映数据页
    buffer_pool_manager_->UnpinPage(true_page->GetTablePageId(),false);
    if(gettuple_result)
        //读取成功unpin后返回true
        return true;
        //其余返回false
    else
        return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    if (page_id != INVALID_PAGE_ID) {
        auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
            DeleteTable(temp_table_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    } else {
#ifdef USE_FREESPACE_MAP
      delete freespace_map_;
#endif
        DeleteTable(first_page_id_);
    }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    RowId rid;
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    if (page != nullptr) {
        page->RLatch();
        if (page->GetFirstTupleRid(&rid)) {
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
            return TableIterator(this, rid, txn);
        }
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
    return End();
}
/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    return TableIterator(nullptr, RowId(INVALID_PAGE_ID, 0), nullptr);
}
