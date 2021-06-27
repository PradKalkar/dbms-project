//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    if (comparator(array[i].first, key) == 0) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  assert(index >= 0 && index < GetSize());
  array[index].second = value;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int size = GetSize();
  for (int i = 1; i < size; i++) {
    if (comparator(array[i].first, key) > 0) {
      return array[i - 1].second;
    }
  }
  // if key is greater than all of the keys inside the page, then return the last pointer
  return array[size - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1] = {new_key, new_value};
  SetParentPageId(INVALID_PAGE_ID);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int index = ValueIndex(old_value) + 1;
  assert(index > 0);
  IncreaseSize(1);
  int size = GetSize();
  for (int i = size - 1; i > index; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }
  array[index].first = new_key;
  array[index].second = new_value;
  return size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  int total = GetMaxSize() + 1;
  assert(GetSize() == total);

  // copy last half
  int copyIdx = total / 2;
  page_id_t recipPageId = recipient->GetPageId();

  for (int i = copyIdx; i < total; i++) {
    recipient->SetKeyAt(i - copyIdx, array[i].first);
    recipient->SetValueAt(i - copyIdx, array[i].second);

    // update children's parent page
    auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
    assert(childRawPage != nullptr);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
    childTreePage->SetParentPageId(recipPageId);
    buffer_pool_manager->UnpinPage(array[i].second, true);
  }
  // set size,is odd, bigger is last part
  SetSize(copyIdx);
  recipient->SetSize(total - copyIdx);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int copyIdx = GetSize();

  for (int i = 0; i < size; i++) {
    array[copyIdx + i] = items[i];

    // update children's parent page
    auto childRawPage = buffer_pool_manager->FetchPage(array[copyIdx + i].second);
    assert(childRawPage != nullptr);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
    childTreePage->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(array[copyIdx + i].second, true);
  }
  // set size,is odd, bigger is last part
  SetSize(copyIdx + size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  assert(index >= 0 && index < size);
  for (int i = index + 1; i < size; i++) {
    array[i - 1] = array[i];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType ret = ValueAt(0);
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int start = recipient->GetSize();
  int size = GetSize();
  page_id_t recipPageId = recipient->GetPageId();

  // the separation key from parent
  SetKeyAt(0, middle_key);
  for (int i = 0; i < size; ++i) {
    recipient->SetKeyAt(start + i, array[i].first);
    recipient->SetValueAt(start + i, array[i].second);

    // update children's parent page
    auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
    assert(childRawPage != nullptr);
    BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
    childTreePage->SetParentPageId(recipPageId);
    buffer_pool_manager->UnpinPage(array[i].second, true);
  }

  // update relavent sizes in the current page and the recipient page.
  recipient->SetSize(start + size);
  assert(recipient->GetSize() <= GetMaxSize());

  SetSize(0);
  buffer_pool_manager->UnpinPage(GetPageId(), true);
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager,
                                                      const KeyComparator &comparator) {
  // pseudocode -
  // diagram of any node - K0 - P0 - K1 - P1 - K2 - P2 - ...
  // rem_key = array[1].first
  // rem_ptr = array[0].second
  // move all the contents of curr node to left
  // insertIntoRecipient(middle_ley, rem_ptr)
  // Find index of middle key in parent - ind
  // for middle key the left ptr is parent->arr[ind-1].second and right ptr is parent->arr[ind].second - no need to
  // update these parent->array[ind].first = rem_key;

  assert(GetSize() > 0);
  KeyType removed_key = array[1].first;
  ValueType removed_pointer = array[0].second;
  IncreaseSize(-1);

  int curr_size = GetSize();
  for (int i = 0; i < curr_size; i++) {
    array[i] = array[i + 1];  // moving contents to left
  }

  // update child parent_page_id_ and copy the last
  recipient->CopyLastFrom({middle_key, removed_pointer}, buffer_pool_manager);

  // update relevant key in its parent page.
  auto page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page != nullptr);
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  int middle_index = parent->KeyIndex(middle_key, comparator);
  parent->SetKeyAt(middle_index, removed_key);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = pair;

  Page *childRawPage = buffer_pool_manager->FetchPage(pair.second);
  assert(childRawPage != nullptr);
  BPlusTreePage *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());

  childTreePage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(childTreePage->GetPageId(), true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager,
                                                       const KeyComparator &comparator) {
  assert(GetSize() > 0);
  KeyType removed_key = array[GetSize() - 1].first;
  ValueType removed_pointer = array[GetSize() - 1].second;
  IncreaseSize(-1);

  // update child parent_page_id_ and copy the last
  recipient->CopyFirstFrom({middle_key, removed_pointer}, buffer_pool_manager);

  // update relevant key in its parent page.
  auto page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page != nullptr);
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  int middle_index = parent->KeyIndex(middle_key, comparator);
  parent->SetKeyAt(middle_index, removed_key);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  assert(size < GetMaxSize());

  for (int i = 0; i < size; i++) {
    array[i + 1] = array[i];  // moving contents to right
  }

  // Insert into the first position
  array[0].second = pair.second;
  array[1].first = pair.first;

  // Change child page
  auto page = buffer_pool_manager->FetchPage(pair.second);
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());

  // unpin pages
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
