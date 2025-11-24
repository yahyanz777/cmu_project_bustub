//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"

namespace bustub {

ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> guard(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  auto try_evict = [&](std::list<frame_id_t> &alive_list, std::list<page_id_t> &ghost_list,
                       ArcStatus status) -> std::optional<frame_id_t> {
    // iterate from LRU (back) to MRU (front)
    for (auto it = alive_list.end(); it != alive_list.begin();) {
      --it;
      frame_id_t f_id = *it;

      auto mit = alive_map_.find(f_id);
      if (mit == alive_map_.end()) {
        // inconsistency: remove the stale entry from the list
        it = alive_list.erase(it);
        continue;
      }

      auto fs = mit->second;  // copy shared_ptr BEFORE any erasure
      if (!fs->evictable_) {
        // pinned, skip
        continue;
      }

      // copy needed data before erasing
      page_id_t p_id = fs->page_id_;

      // erase the list element (it currently points to element to erase)
      it = alive_list.erase(it);

      // erase map entry
      alive_map_.erase(mit);

      // update count
      if (curr_size_ > 0) {
        --curr_size_;
      }

      // move to ghost list (most recent at front)
      ghost_list.push_front(p_id);
      ghost_map_[p_id] = std::make_shared<FrameStatus>(p_id, static_cast<frame_id_t>(-1), false, status);

      // trim at most one ghost entry if exceeded capacity
      size_t all_ghost_size = mru_ghost_.size() + mfu_ghost_.size();
      if (all_ghost_size > replacer_size_) {
        if (mru_ghost_.size() >= mfu_ghost_.size()) {
          page_id_t pid = mru_ghost_.back();
          mru_ghost_.pop_back();
          ghost_map_.erase(pid);
        } else {
          page_id_t pid = mfu_ghost_.back();
          mfu_ghost_.pop_back();
          ghost_map_.erase(pid);
        }
      }

      return f_id;
    }

    return std::nullopt;
  };

  bool prefer_mru = (mru_.size() >= mru_target_size_);
  if (prefer_mru) {
    if (auto res = try_evict(mru_, mru_ghost_, ArcStatus::MRU_GHOST)) return res;
    if (auto res = try_evict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST)) return res;
  } else {
    if (auto res = try_evict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST)) return res;
    if (auto res = try_evict(mru_, mru_ghost_, ArcStatus::MRU_GHOST)) return res;
  }

  return std::nullopt;
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);

  // Case 1: hit in alive lists (MRU or MFU)
  auto it_alive = alive_map_.find(frame_id);
  if (it_alive != alive_map_.end()) {
    auto fs = it_alive->second;
    // remove from its current alive list
    if (fs->arc_status_ == ArcStatus::MRU) {
      mru_.remove(frame_id);
    } else {
      mfu_.remove(frame_id);
    }
    // promote to MFU
    fs->arc_status_ = ArcStatus::MFU;
    fs->page_id_ = page_id;
    mfu_.push_front(frame_id);
    return;
  }

  // Case 2/3: ghost hit
  auto it_ghost = ghost_map_.find(page_id);
  if (it_ghost != ghost_map_.end()) {
    auto gfs = it_ghost->second;
    bool was_mru_ghost = (gfs->arc_status_ == ArcStatus::MRU_GHOST);

    // adjust target size p
    if (was_mru_ghost) {
      mru_target_size_ = std::min(mru_target_size_ + 1, replacer_size_);
    } else {
      if (mru_target_size_ > 0) mru_target_size_--;
    }

    // remove from ghost lists and ghost_map
    mru_ghost_.remove(page_id);
    mfu_ghost_.remove(page_id);
    ghost_map_.erase(page_id);

    // add to MFU alive
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    mfu_.push_front(frame_id);
    return;
  }

  // Case 4: miss in all lists
  size_t alive_total = alive_map_.size();
  size_t ghost_total = ghost_map_.size();

  // Case 4(a)
  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    if (!mru_ghost_.empty()) {
      page_id_t old = mru_ghost_.back();
      mru_ghost_.pop_back();
      ghost_map_.erase(old);
    }
  }
  // Case 4(b)
  else if (alive_total + ghost_total == 2 * replacer_size_) {
    if (!mfu_ghost_.empty()) {
      page_id_t old = mfu_ghost_.back();
      mfu_ghost_.pop_back();
      ghost_map_.erase(old);
    }
  }
  mru_.push_front(frame_id);
alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);

}


void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    throw std::runtime_error("SetEvictable() called on invalid frame_id");
  }
  bool curr_evictable = it->second->evictable_;
  if (curr_evictable != set_evictable) {
    it->second->evictable_ = set_evictable;
    if (set_evictable) {
      ++curr_size_;
    } else {
      if (curr_size_ > 0) --curr_size_;
    }
  }
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) return;
  if (!it->second->evictable_) {
    throw std::runtime_error("Remove() called on non-evictable frame");
  }

  ArcStatus status = it->second->arc_status_;
  if (status == ArcStatus::MFU) {
    mfu_.remove(frame_id);
  } else if (status == ArcStatus::MRU) {
    mru_.remove(frame_id);
  }

  alive_map_.erase(it);
  if (curr_size_ > 0) --curr_size_;
}

auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
