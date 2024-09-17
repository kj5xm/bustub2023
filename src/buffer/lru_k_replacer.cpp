//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  history_end_ptr_ = new LRUKNode(-1, 0);
  middle_separator_ptr_ = new LRUKNode(-2, 0);
  buffer_start_ptr_ = new LRUKNode(-3, 0);

  history_end_ptr_->frontptr_ = middle_separator_ptr_;
  history_end_ptr_->backptr_ = nullptr;
  middle_separator_ptr_->frontptr_ = buffer_start_ptr_;
  middle_separator_ptr_->backptr_ = history_end_ptr_;
  buffer_start_ptr_->frontptr_ = nullptr;
  buffer_start_ptr_->backptr_ = middle_separator_ptr_;

  for (size_t i = 0; i < num_frames; i++) {
    node_store_[static_cast<int>(i)] = new LRUKNode(static_cast<frame_id_t>(i), k_);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  if (curr_size_ == 0) {
    return false;
  }

  if (!IsHistoryEmpty()) {
    for (auto node_ptr = middle_separator_ptr_->backptr_; node_ptr != history_end_ptr_; node_ptr = node_ptr->backptr_) {
      if (node_ptr->GetEvictable()) {
        DisLink(node_ptr);
        *frame_id = node_ptr->GetFrameID();
        node_ptr->SetEvictable(false);
        node_ptr->CleanHistory();
        curr_size_--;
        if (is_debug_) {
          DebugPrint();
        }
        return true;
      }
    }
  }

  if (!IsBufferEmpty()) {
    for (auto node_ptr = buffer_start_ptr_->backptr_; node_ptr != middle_separator_ptr_;
         node_ptr = node_ptr->backptr_) {
      if (node_ptr->GetEvictable()) {
        DisLink(node_ptr);
        *frame_id = node_ptr->GetFrameID();
        node_ptr->SetEvictable(false);
        node_ptr->CleanHistory();
        curr_size_--;
        if (is_debug_) {
          DebugPrint();
        }
        return true;
      }
    }
  }

  if (is_debug_) {
    DebugPrint();
  }
  return false;
}

void LRUKReplacer::DisLink(LRUKNode *node_ptr) {
  auto node_back = node_ptr->backptr_;
  auto node_front = node_ptr->frontptr_;

  if (node_back != nullptr) {
    node_back->frontptr_ = node_front;
  }
  if (node_front != nullptr) {
    node_front->backptr_ = node_back;
  }

  node_ptr->frontptr_ = nullptr;
  node_ptr->backptr_ = nullptr;
}

void LRUKReplacer::MoveToEnd(LRUKNode *node_ptr, LRUKNode *end_node_ptr) {
  DisLink(node_ptr);
  auto end_node_front_ptr = end_node_ptr->frontptr_;
  end_node_ptr->frontptr_ = node_ptr;
  if (end_node_front_ptr != nullptr) {
    end_node_front_ptr->backptr_ = node_ptr;
  }
  node_ptr->frontptr_ = end_node_front_ptr;
  node_ptr->backptr_ = end_node_ptr;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  current_timestamp_++;
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw ExecutionException("LRUKReplacer::RecordAccess: The frame_id is larger than the replacer size!");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // Create a new node if it doesn't exist
    auto new_node = new LRUKNode(frame_id, k_);
    node_store_[frame_id] = new_node;
    new_node->RecordAccess(current_timestamp_);
    MoveToEnd(new_node, history_end_ptr_);
  } else {
    auto node_ptr = it->second;
    if (node_ptr->RecordAccess(current_timestamp_)) {
      MoveToEnd(node_ptr, middle_separator_ptr_);
    } else {
      if (node_ptr->GetSize() < k_) {
        MoveToEnd(node_ptr, history_end_ptr_);
      } else {
        MoveToEnd(node_ptr, middle_separator_ptr_);
      }
    }
  }

  if (is_debug_) {
    DebugPrint();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);

  if (node_store_.count(frame_id) != 0 && node_store_.at(frame_id)->GetEvictable() != set_evictable) {
    node_store_.at(frame_id)->SetEvictable(set_evictable);
    curr_size_ += set_evictable ? 1 : -1;
  }

  if (is_debug_) {
    DebugPrint();
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  if (!it->second->GetEvictable()) {
    throw ExecutionException("LRUKReplacer::Remove: The frame_id is not evictable!");
  }

  auto node_ptr = it->second;
  DisLink(node_ptr);
  delete node_ptr;
  node_store_.erase(it);

  curr_size_--;
  if (is_debug_) {
    DebugPrint();
  }
}

auto LRUKReplacer::Size() const -> size_t { return curr_size_; }

auto LRUKReplacer::IsBufferEmpty() const -> bool { return middle_separator_ptr_->frontptr_ == buffer_start_ptr_; }

auto LRUKReplacer::IsHistoryEmpty() const -> bool { return history_end_ptr_->frontptr_ == middle_separator_ptr_; }

void LRUKReplacer::DebugPrint() const {
  std::cout << "--------------------------------------------------" << std::endl;
  for (auto node_ptr = history_end_ptr_; node_ptr != nullptr; node_ptr = node_ptr->frontptr_) {
    std::cout << "node=" << node_ptr->GetFrameID() << ", size=" << node_ptr->GetSize()
              << ", evictable=" << node_ptr->GetEvictable() << std::endl;
  }
}

}  // namespace bustub
