#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : rid_(rid), table_heap_(table_heap), txn_(txn), page_(nullptr), row_(new Row(rid)) {
  // 如果 RowId 有效，获取对应的 TablePage
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(row_, txn_);
    page_ = table_heap_->FetchPage(rid.GetPageId());
  }
}

TableIterator::TableIterator(const TableIterator &other)
    : rid_(other.rid_), table_heap_(other.table_heap_), txn_(other.txn_), page_(other.page_), row_(new Row(*other.row_)) {}

TableIterator::~TableIterator() {
  delete row_;
  if (page_ != nullptr) {
    table_heap_->UnpinPage(page_->GetPageId(), false);
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (row_->GetRowId() == itr.row_->GetRowId());
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(this->rid_ == itr.rid_ && this->page_ == itr.page_);
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
    row_ = new Row(*itr.row_);
    txn_ = itr.txn_;
    rid_ = itr.rid_;
    page_ = itr.page_;
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
  bool first_slot=false;
  while (true) {
    // 移动到当前页面中的下一条记录
    if(!first_slot)rid_ = RowId(rid_.GetPageId(), rid_.GetSlotNum() + 1);
    first_slot = false;
    // 检查是否到达页面中的记录末尾
    if (rid_.GetSlotNum() < page_->GetTupleCount()) {
      // 如果槽位不是空闲的，则找到下一条有效记录
      if (!page_->IsDeleted(page_->GetTupleSize(rid_.GetSlotNum()))) {
#ifdef ENABLE_TABLEHEAP_ITER_DEBUG
        LOG(INFO)<<"GET "<<rid_.GetPageId()<<' '<<rid_.GetSlotNum()<<endl;
#endif
        row_->SetRowId(rid_);
        table_heap_->GetTuple(row_, txn_);
        table_heap_->UnpinPage(page_->GetPageId(),false);//forget unpin
        break;
      }
    } else {
      // 移动到下一页
      page_id_t next_page_id = page_->GetNextPageId();
#ifdef ENABLE_TABLEHEAP_ITER_DEBUG
      LOG(INFO)<<"COME TO NEXT PAGE"<<endl;
#endif
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
      first_slot = true;
    }
  }
}

