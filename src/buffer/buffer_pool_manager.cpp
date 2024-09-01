//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //    "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  Page* new_page = nullptr;
  std::scoped_lock scoped_manager_latch(latch_);

  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    *page_id = AllocatePage();
    new_page = &pages_[static_cast<int>(frame_id)];
    new_page->page_id_ = *page_id;
    page_table_.emplace(*page_id, frame_id);
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].IsDirty()) {
      page_id_t old_page_id = pages_[frame_id].GetPageId();
      auto promise_write = disk_scheduler_->CreatePromise();
      auto future_write = promise_write.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise_write)});

      if (!future_write.get()) {
        throw ExecutionException("BufferPoolManager::NewPage: write page error!");
      }

      pages_[frame_id].ResetMemory();
      page_table_.erase(page_table_.find(old_page_id));
    }

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    *page_id = AllocatePage();
    new_page = &pages_[static_cast<int>(frame_id)];
    new_page->page_id_ = *page_id;
    page_table_.emplace(*page_id, frame_id);
  }

  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock scoped_manager_latch(latch_);
  frame_id_t  frame_id;
  Page* fetch_page = nullptr;

  //if the page in the buffer pool, return the page
  if (page_table_.count(page_id)) {
    frame_id = page_table_.at(page_id);
    return &pages_[static_cast<int>(frame_id)];
  } else {
    //else 1. create a new page from free_list_ 2. evict a page from replacer_
    if (!free_list_.empty()) {
      frame_id = free_list_.back();
      free_list_.pop_back();

      auto promise_read = disk_scheduler_->CreatePromise();
      auto future_read = promise_read.get_future();
      disk_scheduler_->Schedule({false, pages_[static_cast<int>(frame_id)].GetData(), page_id, std::move(promise_read)});

      if (!future_read.get()) {
        throw ExecutionException("BufferPoolManager::FetchPage: read page error!");
      }

      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);

      fetch_page = &pages_[static_cast<int>(frame_id)];
      fetch_page->page_id_ = page_id;
      page_table_.emplace(page_id, frame_id);
    } else if (replacer_->Size() > 0){
      replacer_->Evict(&frame_id);
      page_id_t old_page_id = pages_[frame_id].GetPageId();
      page_table_.erase(page_table_.find(old_page_id));
      if (pages_[frame_id].IsDirty()) {
        auto promise_write = disk_scheduler_->CreatePromise();
        auto future_write = promise_write.get_future();
        disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise_write)});

        if (!future_write.get()) {
          throw ExecutionException("BufferPoolManager::FetchPage: write page error!");
        }
        pages_[frame_id].ResetMemory();
      }

      auto promise_read = disk_scheduler_->CreatePromise();
      auto future_read = promise_read.get_future();
      disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), page_id, std::move(promise_read)});
      if (!future_read.get()) {
        throw ExecutionException("BufferPoolManager::FetchPage: read page error!");
      }

      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);

      fetch_page = &pages_[static_cast<int>(frame_id)];
      fetch_page->page_id_ = page_id;
      page_table_.emplace(page_id, frame_id);
    }
  }

  return fetch_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock scoped_manager_latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (is_dirty && !pages_[frame_id].is_dirty_) {
    pages_[frame_id].is_dirty_ = true;
  }

  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].pin_count_--;
  }

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock scoped_manager_latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  auto promise_write = disk_scheduler_->CreatePromise();
  auto future_write = promise_write.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), page_id, std::move(promise_write)});
  if (!future_write.get()) {
    throw ExecutionException("BufferPoolManager::FlushPage: write page error!");
  }
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock scoped_manager_latch(latch_);
  for (auto page_iter : page_table_) {
    page_id_t page_id = page_iter.first;
    frame_id_t frame_id = page_iter.second;
    auto promise_write = disk_scheduler_->CreatePromise();
    auto future_write = promise_write.get_future();
    disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), page_id, std::move(promise_write)});
    if (!future_write.get()) {
      throw ExecutionException("BufferPoolManager::FlushAllPage: write page error!");
    }
    pages_[frame_id].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock scoped_manager_latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_.at(page_id);
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_table_.find(page_id));
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
