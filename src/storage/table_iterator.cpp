#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
        : table_heap_(table_heap), txn_(txn), row_(new Row(rid)) {
    // 如果 RowId 有效，获取对应的 TablePage
    if (rid.GetPageId() != INVALID_PAGE_ID) {
        table_heap_->GetTuple(row_,txn_);
    }
}

TableIterator::TableIterator(const TableIterator &other)
        : table_heap_(other.table_heap_), txn_(other.txn_), row_(other.row_) {}


TableIterator::~TableIterator() {
    delete row_;
}


bool TableIterator::operator==(const TableIterator &itr) const {
    return (row_->GetRowId() == itr.row_->GetRowId());
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    return !(*this == itr);
}

const Row &TableIterator::operator*() {
    return *row_;
}

Row *TableIterator::operator->() {
  return this->row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    if (this != &itr) {
        table_heap_ = itr.table_heap_;
        row_ = itr.row_;
        txn_ = itr.txn_;
    }
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
    MoveToNextTuple();
    return *this;
}


// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator temp(*this);
    ++(*this);
    return temp;
}

void TableIterator::MoveToNextTuple() {
    if (page_ == nullptr) {
        return;
    }
    while (true) {
        // 移动到当前页面中的下一条记录
        rid_ = RowId(rid_.GetPageId(), rid_.GetSlotNum() + 1);
        // 检查是否到达页面中的记录末尾
        if (rid_.GetSlotNum() < page_->GetTupleCount()) {
            // 如果槽位不是空闲的，则找到下一条有效记录
            if (!page_->IsDeleted(rid_.GetSlotNum())) {
                break;
            }
        } else {
            // 移动到下一页
            page_id_t next_page_id = page_->GetNextPageId();
            if (next_page_id == INVALID_PAGE_ID) {
                // 没有更多页面，迭代器到达末尾
                page_ = nullptr;
                rid_ = RowId(INVALID_PAGE_ID, 0);
                return;
            }
            // 释放当前页面，并获取下一页
            table_heap_->UnpinPage(page_->GetPageId(), false);
            page_ = table_heap_->FetchPage(next_page_id);
            rid_ = RowId(next_page_id, 0);
        }
    }
}