//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  latch_.lock();
  // 1.     Search the page table for the requested page (P).
  auto it = page_table_.find(page_id);

  if (it != page_table_.end()) {
    // 1.1    If P exists, pin it and return it immediately.
    auto frameId = it->second;
    replacer_->Pin(frameId);
    pages_[frameId].pin_count_++;  // increment the pin count of the page
    latch_.unlock();
    return &pages_[frameId];
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame;
  if (free_list_.empty()) {
    // we need to remove least recently used page
    bool done = replacer_->Victim(&frame);
    if (!done) {
      // we do not have any frame to be replaced, all are pinned
      latch_.unlock();
      return nullptr;
    }
    // 2.     If R is dirty, write it back to the disk.
    if (pages_[frame].IsDirty()) {
      disk_manager_->WritePage(pages_[frame].GetPageId(), pages_[frame].GetData());
    }
  } else {
    // fetch the frame from free list
    frame = free_list_.back();
    free_list_.pop_back();
  }

  // 3.     Delete R from the page table and insert P.
  if (page_table_.count(pages_[frame].GetPageId()) != 0U) {
    page_table_.erase(pages_[frame].GetPageId());
  }

  page_table_[page_id] = frame;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto &page = pages_[frame];
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  replacer_->Pin(frame);
  page.is_dirty_ = false;
  page.ResetMemory();
  disk_manager_->ReadPage(page_id, page.GetData());

  latch_.unlock();
  return &pages_[frame];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {  // page is not there in the page table
    latch_.unlock();
    return false;
  }

  auto &page = pages_[it->second];  // it->second is the frameId associated with page_id
  if (page.pin_count_ > 0) {
    page.pin_count_--;
    if (page.pin_count_ == 0) {
      replacer_->Unpin(it->second);
    }
  }
  page.is_dirty_ |= is_dirty;
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || page_id == INVALID_PAGE_ID) {
    // the page is not in the buffer
    latch_.unlock();
    return false;
  }

  auto frameId = it->second;
  disk_manager_->WritePage(page_id, pages_[frameId].GetData());
  pages_[frameId].is_dirty_ = false;

  latch_.unlock();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  latch_.lock();
  *page_id = INVALID_PAGE_ID;

  // 1.2    Find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t frame;
  if (free_list_.empty()) {
    // we need to remove least recently used page
    bool done = replacer_->Victim(&frame);
    if (!done) {
      latch_.unlock();
      return nullptr;
    }
    // 2.     If R is dirty, write it back to the disk.
    if (pages_[frame].IsDirty()) {
      disk_manager_->WritePage(pages_[frame].GetPageId(), pages_[frame].GetData());
    }
  } else {
    // fetch the frame from free list
    frame = free_list_.back();
    free_list_.pop_back();
  }

  page_id_t newPID = disk_manager_->AllocatePage();

  // 3.     Delete R from the page table and insert P.
  if (page_table_.count(pages_[frame].GetPageId()) != 0U) {
    page_table_.erase(pages_[frame].GetPageId());
  }

  page_table_[newPID] = frame;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto &page = pages_[frame];
  page.page_id_ = newPID;
  page.pin_count_ = 1;
  replacer_->Pin(frame);
  page.is_dirty_ = false;
  page.ResetMemory();

  *page_id = newPID;

  latch_.unlock();
  return &pages_[frame];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return true;
  }

  frame_id_t frameId = page_table_[page_id];
  auto &page = pages_[frameId];
  if (page.pin_count_ > 0) {
    latch_.unlock();
    return false;  // someone is using this page
  }

  page_table_.erase(page_id);
  if (page.IsDirty()) {
    disk_manager_->WritePage(page.GetPageId(), page.GetData());
  }
  disk_manager_->DeallocatePage(page_id);

  // removing the frame from the LRU replacer since it doesnt contain any page now
  // pin will basically remove it
  replacer_->Pin(frameId);

  // resetting the metadata
  page.page_id_ = INVALID_PAGE_ID;
  page.ResetMemory();
  page.is_dirty_ = false;
  page.pin_count_ = 0;

  free_list_.push_back(frameId);  // now the frame with frameId has become free

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t fid = 0; fid < pool_size_; fid++) {
    FlushPageImpl(pages_[fid].GetPageId());
  }
}

}  // namespace bustub
