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

#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) :capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
	latch_.lock();
  // 如果可替换的list为空则返回false
  if (LRU_List_.size() == 0) {
    latch_.unlock();
    return false;
  }

  *frame_id = LRU_List_.back();
  LRU_List_.pop_back();
  latch_.unlock();

	return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();

  LRU_List_.remove(frame_id);
  
  latch_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  
  latch_.lock();
  if (std::find(LRU_List_.begin(), LRU_List_.end(), frame_id) != LRU_List_.end()) {
    latch_.unlock();
    return;
  }

  while (Size() >= capacity_) {
    LRU_List_.pop_back();
  }

  LRU_List_.push_front(frame_id);
  latch_.unlock();
}

size_t LRUReplacer::Size() { return LRU_List_.size(); }

}  // namespace bustub
