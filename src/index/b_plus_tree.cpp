#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id), buffer_pool_manager_(buffer_pool_manager), processor_(KM) {
  auto leaf_max_size_cal = ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (sizeof(RowId) + KM.GetKeySize()) - 1);
  auto internal_max_size_cal = ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(RowId) + KM.GetKeySize()) - 1);
  if (leaf_max_size != UNDEFINED_SIZE)
    leaf_max_size_ = leaf_max_size;
  else
    leaf_max_size_ = leaf_max_size_cal;

  if (internal_max_size != UNDEFINED_SIZE)
    internal_max_size_ = internal_max_size;
  else
    internal_max_size_ = internal_max_size_cal;
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
  }
  if (current_page_id == INVALID_PAGE_ID) {
    return;
  }
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr) {
    return;
  }
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (tree_page->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);

  } else {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(tree_page);
    for (int i = 0; i < internal_page->GetSize(); ++i) {
      Destroy(internal_page->ValueAt(i));
    }
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key);
  cout << "PageId: " << page->GetPageId() << endl;
  cout << endl;
  if (page == nullptr) {
    return false;
  }
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  RowId value;
  bool found = leaf->Lookup(key, value, processor_);
  if (found) {
    result.push_back(value);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t page_id;
  Page *root_page = buffer_pool_manager_->NewPage(page_id);
  if (root_page == nullptr) {
    throw std::runtime_error("Out of memory");
  }
  root_page_id_ = page_id;
  UpdateRootPageId(1);

  LeafPage *root = reinterpret_cast<LeafPage *>(root_page->GetData());
  root->Init(page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  root->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  Page *page = FindLeafPage(key);
  if (page == nullptr) {
    return false;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  RowId temp_value;  // Use a temporary non-const variable
  if (leaf_page->Lookup(key, temp_value, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  leaf_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  if (leaf_page->GetSize() > leaf_max_size_) {
    LeafPage *new_leaf_page = Split(leaf_page, transaction);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  InternalPage *new_node = reinterpret_cast<InternalPage *>(page->GetData());
  new_node->Init(page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("out of memory");
  }
  LeafPage *new_node = reinterpret_cast<LeafPage *>(page->GetData());
  new_node->Init(page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_node);
  new_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_node->GetPageId());
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t page_id;
    Page *page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr) {
      throw std::runtime_error("out of memory");
    }
    InternalPage *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(page_id, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
    root->SetValueAt(0, old_node->GetPageId());
    root->SetKeyAt(1, key);
    root->SetValueAt(1, new_node->GetPageId());
    root->SetSize(2);

    old_node->SetParentPageId(root->GetPageId());
    new_node->SetParentPageId(root->GetPageId());
    root_page_id_ = root->GetPageId();
    UpdateRootPageId();

    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    return;
  }
  page_id_t parent_id = old_node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (parent->GetSize() > parent->GetMaxSize()) {
    InternalPage *new_parent = Split(parent, transaction);
    GenericKey *new_key = new GenericKey();
    *new_key = *new_parent->KeyAt(0);
    InsertIntoParent(parent, new_key, new_parent, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  std::cout << "root_page_id: " << root_page_id_ << std::endl;
  Page *page = FindLeafPage(key);
  if (page == nullptr) {
    return;
  }
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  leaf->RemoveAndDeleteRecord(key, processor_);
  if (leaf->GetSize() < leaf->GetMinSize()) {
    std::cout << "<" << endl;
    CoalesceOrRedistribute(leaf, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  page_id_t parent_id = node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  int sibling_index = index == 0 ? 1 : index - 1;
  N *sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(sibling_index))->GetData());
  bool should_delete = false;
  if (node->GetSize() + sibling->GetSize() < node->GetMaxSize()) {
    if (index == 0) {
      should_delete = Coalesce(sibling, node, parent, sibling_index, transaction);
    } else {
      should_delete = Coalesce(node, sibling, parent, index, transaction);
    }
  } else {
    Redistribute(sibling, node, index);
  }
  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
  return should_delete;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index == 0) {
    std::swap(neighbor_node, node);
    index = 1;
  }
  std::cout << "neighbor_node: " << neighbor_node->GetPageId() << std::endl;
  std::cout << "node: " << node->GetPageId() << std::endl;

  node->MoveAllTo(neighbor_node);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  parent->Remove(index);
  // Page *page = buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1));
  // BPlusTreeLeafPage *new_r = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
  // parent->SetKeyAt(index - 1, new_r->KeyAt(0));
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index == 0) {
    std::swap(neighbor_node, node);
    index = 1;
  }
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  page_id_t parent_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());

  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(parent_id, false);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  page_id_t parent_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());

  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
  }

  buffer_pool_manager_->UnpinPage(parent_id, false);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() > 1) {
    return false;
  }
  if (old_root_node->IsLeafPage()) {
    root_page_id_ = INVALID_PAGE_ID;
  } else {
    InternalPage *root = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = root->ValueAt(0);
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_r = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (new_r->IsLeafPage() && new_r->GetSize() == 0) {
      root_page_id_ = root->ValueAt(1);
      Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    }
    if (page != nullptr) {
      BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
  }
  buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page *page = FindLeafPage(nullptr, root_page_id_, true);
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page *page = FindLeafPage(key);
  int index = reinterpret_cast<LeafPage *>(page->GetData())->KeyIndex(key, processor_);
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (IsEmpty()) {
    return nullptr;
  }
  if (page_id == INVALID_PAGE_ID) {
    page_id = root_page_id_;
  }
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    InternalPage *internal = reinterpret_cast<InternalPage *>(node);
    page_id = leftMost ? internal->ValueAt(0) : internal->Lookup(key, processor_);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(page->GetData());
  if (insert_record) {
    root_page->Insert(index_id_, root_page_id_);
  } else {
    root_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">" << "max_size=" << leaf->GetMaxSize()
        << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << "Field: " << ans.GetField(0)->toString() << " PageId: " << leaf->ValueAt(i).GetPageId()
          << " SlotNum: " << leaf->ValueAt(i).GetSlotNum() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">" << "max_size=" << inner->GetMaxSize()
        << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
