//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // step 1. find page
  B_PLUS_TREE_LEAF_PAGE_TYPE *tar = FindLeafPage(key);
  if (tar == nullptr) {
    return false;
  }

  // step 2. find value
  result->resize(1);
  auto ret = tar->Lookup(key, &(result->at(0)), comparator_);

  // step 3. unPin buffer pool
  buffer_pool_manager_->UnpinPage(tar->GetPageId(), false);
  return ret;
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
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  bool res = InsertIntoLeaf(key, value, transaction);

  return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // step 1. ask for new page from buffer pool manager
  page_id_t newPageId;
  Page *rootPage = buffer_pool_manager_->NewPage(&newPageId);
  assert(rootPage != nullptr);

  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootPage->GetData());

  // step 2. update b+ tree's root page id
  root->Init(newPageId, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = newPageId;
  UpdateRootPageId(true);

  // step 3. insert entry directly into leaf page.
  root->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(newPageId, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage = FindLeafPage(key);
  ValueType v;

  bool exist = leafPage->Lookup(key, &v, comparator_);
  if (exist) {
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
    return false;
  }

  leafPage->Insert(key, value, comparator_);
  if (leafPage->GetSize() > leafPage->GetMaxSize()) {           // insert then split
    B_PLUS_TREE_LEAF_PAGE_TYPE *newLeafPage = Split(leafPage);  // unpin it in below func
    InsertIntoParent(leafPage, newLeafPage->KeyAt(0), newLeafPage, transaction);
  }
  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // step 1 ask for new page from buffer pool manager
  page_id_t newPageId;
  Page *const newPage = buffer_pool_manager_->NewPage(&newPageId);
  assert(newPage != nullptr);

  // step 2 move half of key & value pairs from input page to newly created page
  N *newNode = reinterpret_cast<N *>(newPage->GetData());
  newNode->Init(newPageId, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(newNode, buffer_pool_manager_);
  // fetch page and new page need to unpin page(do it outside)
  return newNode;
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *const newPage = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(newPage != nullptr);
    assert(newPage->GetPinCount() == 1);
    InternalPage *newRoot = reinterpret_cast<InternalPage *>(newPage->GetData());
    newRoot->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    newRoot->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    UpdateRootPageId();
    // fetch page and new page need to unpin page
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(newRoot->GetPageId(), true);
    return;
  }
  page_id_t parentId = old_node->GetParentPageId();
  auto *page = buffer_pool_manager_->FetchPage(parentId);
  assert(page != nullptr);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page);

  InternalPage *new_node1 = reinterpret_cast<InternalPage *>(new_node);
  new_node1->SetParentPageId(parentId);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  // insert new node after old node
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  if (parent->GetSize() > parent->GetMaxSize()) {
    // begin /* Split Parent */
    InternalPage *newLeafPage = Split(parent);  // new page need unpin
    InsertIntoParent(parent, newLeafPage->KeyAt(0), newLeafPage, transaction);
  }
  buffer_pool_manager_->UnpinPage(parentId, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;

  B_PLUS_TREE_LEAF_PAGE_TYPE *delTar = FindLeafPage(key);

  int curSize = delTar->RemoveAndDeleteRecord(key, comparator_);
  if (curSize < delTar->GetMinSize()) {
    CoalesceOrRedistribute(delTar, transaction);
  }
  buffer_pool_manager_->UnpinPage(delTar->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // if (N is the root and N has only one remaining child)
  if (node->IsRootPage()) {
    return AdjustRoot(node);  // make the child of N the new root of the tree and delete N
  }

  // Let node2 be the previous or next child of parent(N)
  N *node2;
  bool nPrevN2 = FindSibling(node, node2);

  BPlusTreePage *parent = FetchPage(node->GetParentPageId());
  assert(parent != nullptr);
  InternalPage *parentPage = reinterpret_cast<InternalPage *>(parent);

  // if (entries in N and N2 can fit in a single nod)
  if (node->GetSize() + node2->GetSize() <= node->GetMaxSize()) {
    if (nPrevN2) {
      std::swap(node, node2);
    }  // assumption node is after node2

    int removeIndex = parentPage->ValueIndex(node->GetPageId());
    Coalesce(node2, node, parentPage, removeIndex, transaction);  // unpin node,node2
    buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
    return true;
  }

  /* Redistribution: borrow an entry from N2 */
  int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
  KeyType middle_key = parentPage->KeyAt(nodeInParentIndex);
  Redistribute(node2, node, nodeInParentIndex, middle_key);  // unpin node,node2
  buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N *node, N *&sibling) {
  auto page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  assert(page != nullptr);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page);

  int index = parent->ValueIndex(node->GetPageId());
  int siblingIndex = index - 1;  // borrow default from left
  if (index == 0) {              // no prev sibling so borrow from right
    siblingIndex = index + 1;
  }
  sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(siblingIndex)));
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  return index == 0;  // index == 0 means right sibling is borrowed
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
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  // check whether we can coalesce
  assert(node->GetSize() + neighbor_node->GetSize() <= neighbor_node->GetMaxSize());

  // move later one to previous one
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);

  page_id_t pId = node->GetPageId();
  buffer_pool_manager_->UnpinPage(pId, true);
  buffer_pool_manager_->DeletePage(pId);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
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
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index, const KeyType &middle_key) {
  if (index == 0) {  // node is at the left
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_, comparator_);
  } else {  // node is at right
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_, comparator_);
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->IsLeafPage()) {  // case 2
    assert(old_root_node->GetSize() == 0);
    assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  if (old_root_node->GetSize() == 1) {  // case 1
    InternalPage *root = reinterpret_cast<InternalPage *>(old_root_node);

    // const page_id_t newRootId = (root->RemoveAndReturnOnlyChild()).GetPageId();
    const page_id_t newRootId = root->RemoveAndReturnOnlyChild();
    root_page_id_ = newRootId;
    UpdateRootPageId();

    // set the new root's parent id "INVALID_PAGE_ID"
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    assert(page != nullptr);
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *newRoot = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());

    newRoot->SetParentPageId(INVALID_PAGE_ID);

    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());

    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType tmp;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(tmp, true);
  Page *leaf_converted_page = reinterpret_cast<Page *>(leaf_page);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_converted_page->GetData());
  auto page_id = leaf->GetPageId();
  return INDEXITERATOR_TYPE(page_id, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, false);
  Page *leaf_converted_page = reinterpret_cast<Page *>(leaf_page);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_converted_page->GetData());
  int index = leaf->KeyIndex(key, comparator_);
  auto page_id = leaf->GetPageId();
  return INDEXITERATOR_TYPE(page_id, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  KeyType key;
  B_PLUS_TREE_LEAF_PAGE_TYPE *page = FindLeafPage(key, true);
  Page *page_convert = reinterpret_cast<Page *>(page);
  auto leaf = reinterpret_cast<LeafPage *>(page_convert->GetData());
  while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
    page_convert = buffer_pool_manager_->FetchPage(leaf->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_convert->GetPageId(), false);
    leaf = reinterpret_cast<LeafPage *>(page_convert->GetData());
  }
  int index = leaf->GetSize() - 1;
  page_id_t page_id_ = leaf->GetPageId();
  return INDEXITERATOR_TYPE(page_id_, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  if (IsEmpty()) return nullptr;

  //, you need to first fetch the page from buffer pool using its unique page_id, then reinterpret cast to either
  // a leaf or an internal page, and unpin the page after any writing or reading operations.

  auto pointer = FetchPage(root_page_id_);
  page_id_t next;
  for (page_id_t cur = root_page_id_; !pointer->IsLeafPage(); cur = next, pointer = FetchPage(cur)) {
    InternalPage *internalPage = static_cast<InternalPage *>(pointer);
    if (leftMost) {
      next = (internalPage->ValueAt(0));
      // next = (internalPage->ValueAt(0)).GetPageId();
      // next = reinterpret_cast<page_id_t>(internalPage->ValueAt(0));
    } else {
      next = (internalPage->Lookup(key, comparator_));
      // next = (internalPage->Lookup(key,comparator_)).GetPageId();
      // next = reinterpret_cast<page_id_t>(internalPage->Lookup(key,comparator_));
    }
    buffer_pool_manager_->UnpinPage(cur, false);
  }
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pointer);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
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
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
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
      ToGraph(child_page, bpm, out);
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
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub