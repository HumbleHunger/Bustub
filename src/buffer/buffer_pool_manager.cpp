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

bool BufferPoolManager::FindFreePage(frame_id_t *frame_id) {
  // 如果空闲链表非空，则直接返回一个空的frame
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // 调用Repalcer获取可以换出的page的frame_id
  if (replacer_->Victim(frame_id)) {
    int replace_page_id = -1;
    for (const auto &p : page_table_) {
      page_id_t pid = p.first;
      frame_id_t fid = p.second;
      if (fid == *frame_id) {
        replace_page_id = pid;
        break;
      }
    }
    // 处理被淘汰的page，写回
    if (replace_page_id != -1) {
      // 获取淘汰的page
      Page *replace_page = &pages_[*frame_id];

      //std::cout  << "LRU淘汰掉frame " <<*frame_id<< "pageid 为 "<< replace_page_id << std::endl;
      // 如果page是dirty，则需要刷入disk
      if (replace_page->is_dirty_) {
        // 如果恢复日志开启
        if (enable_logging) {
          // 使用while对条件进出判断，保证持久化日志的lsn大于此块的最后更新日志lsn，即保证WAL
          while (replace_page->GetLSN() > log_manager_->GetPersistentLSN()) {
            std::promise<void> promise;
            log_manager_->WakeupFlushThread(&promise);
          }
        }
        char *data = replace_page->data_;
        disk_manager_->WritePage(replace_page_id, data);
      }
      memset(replace_page->data_, 0, PAGE_SIZE);
      // 在page_table中删除这条记录
      page_table_.erase(replace_page_id);
    }
    return true;
  }

  //std::cout << "在buffer pool中没有空间，无法从disk载入" <<std::endl; 
  return false;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  //std::cout << "FetchPgImp" <<  std::endl;
  latch_.lock();
  Page *page = NULL;
  
  if (page_table_.count(page_id)) {
    page = &pages_[page_table_[page_id]];
    page->pin_count_++;
    
    replacer_->Pin(page_table_[page_id]); 

    latch_.unlock();
    return page;
  }
  //std::cout << "在buffer pool中没有，从disk载入" <<std::endl; 
  frame_id_t frame_id = -1;
  // 找一块空的page并将目标page的数据载入
  if (FindFreePage(&frame_id)) {
    // 加载目标page
    page = &pages_[frame_id];
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    
    // 更新repalcer和page_table
    replacer_->Pin(frame_id);
    page_table_[page_id] = frame_id;

    latch_.unlock();
    return page;
  }
  
  latch_.unlock();
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  latch_.lock();

  // 如果page不存在则直接返回
  if (!page_table_.count(page_id)) {
    latch_.unlock();
    return false;
  }
  // 获得目标page
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (is_dirty) {
    page->is_dirty_ = true;
  }
  
  if (page->pin_count_ == 0) {
    latch_.unlock();
    return false;
  }

  --page->pin_count_;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // 没有对page本身加锁，在刷新的过程中可能会有新数据写入
  std::lock_guard<std::mutex> lock(latch_);
  if (!page_table_.count(page_id)) {
    return false;
  }

  Page *page = &pages_[page_table_[page_id]];

  // 如果恢复日志开启
  if (enable_logging) {
    // 使用while对条件进出判断，保证持久化日志的lsn大于此块的最后更新日志lsn，即保证WAL
    while (page->GetLSN() > log_manager_->GetPersistentLSN()) {
      std::promise<void> promise;
      log_manager_->WakeupFlushThread(&promise);
    }
  }

  disk_manager_->WritePage(page_id, page->data_);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  //std::cout << "NewPgImp" << std::endl;
  latch_.lock();
  Page* page = nullptr;
  page_id_t new_page_id = disk_manager_->AllocatePage();

  frame_id_t frame_id;
  if (FindFreePage(&frame_id)) {
    // 准备page数据
    page = &pages_[frame_id];
    page->page_id_ =  new_page_id;
    page->pin_count_ = 1;
    page->is_dirty_ = true;

    *page_id = new_page_id;

    // 处理page_table和replace
    replacer_->Pin(frame_id);
    page_table_[new_page_id] = frame_id;

    latch_.unlock();
    return page;
  }

  latch_.unlock();
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  // 如果在frame中不存在则直接返回true
  if (!page_table_.count(page_id)) {
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->pin_count_) {
    latch_.unlock();
    return false;
  }

  if (page->is_dirty_) {
      // 如果恢复日志开启
      if (enable_logging) {
        // 使用while对条件进出判断，保证持久化日志的lsn大于此块的最后更新日志lsn，即保证WAL
        while (page->GetLSN() > log_manager_->GetPersistentLSN()) {
          std::promise<void> promise;
          log_manager_->WakeupFlushThread(&promise);
        }
      }
    disk_manager_->WritePage(page_id, page->data_);
  }
  
  disk_manager_->DeallocatePage(page_id);

  page_table_.erase(page_id);

  page->is_dirty_ = false;
  page->pin_count_= 0;
  page->page_id_= INVALID_PAGE_ID;

  free_list_.push_back(frame_id);

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
