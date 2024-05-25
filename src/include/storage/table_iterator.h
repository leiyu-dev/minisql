#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"
#include "page/table_page.h" // 确保包含 table_page.h


class TableHeap;

class TableIterator {
public:
 // you may define your own constructor based on your member variables
 explicit TableIterator(TableHeap *table_heap, RowId rid, Txn *txn);

// explicit
 TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here

    RowId rid_;              // 当前记录的 RowId
    TableHeap *table_heap_;  // 指向关联的 TableHeap 对象的指针
    Txn *txn_;               // 当前事务的指针
    TablePage *page_;        // 当前页面的指针
    Row *row_;               // 当前行的指针

    void MoveToNextTuple();
};

#endif  // MINISQL_TABLE_ITERATOR_H
