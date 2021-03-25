//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  latch.lock();
  if (usedFrames.empty()) {
    // *frame_id = INVALID_PAGE_ID;
    latch.unlock();
    return false;
  }
  frame_id_t entry = usedFrames.back();  // this one is the least recently used
  usedFrames.pop_back();
  *frame_id = entry;

  latch.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch.lock();

  auto frameIt = std::find(usedFrames.begin(), usedFrames.end(), frame_id);
  if (frameIt == usedFrames.end()) {
    // frame does not exist!
    latch.unlock();
    return;
  }
  // remove frameIt
  usedFrames.erase(frameIt);
  latch.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  latch.lock();
  auto frameIt = std::find(usedFrames.begin(), usedFrames.end(), frame_id);
  if (frameIt != usedFrames.end()) {
    // frame exists!
    latch.unlock();
    return;
  }
  usedFrames.push_front(frame_id);
  latch.unlock();
}

size_t LRUReplacer::Size() { return usedFrames.size(); }

}  // namespace bustub
