//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"

#include "common/config.h"

namespace bustub {

ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> guard(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  // Try to evict from a given alive list and move its page into the given
  // ghost list.
  auto try_evict = [&](std::list<frame_id_t> &alive_list, std::list<page_id_t> &ghost_list,
                       ArcStatus ghost_status) -> std::optional<frame_id_t> {
    // Iterate from LRU (back) to MRU (front) to find evictable frame
    for (auto it = alive_list.rbegin(); it != alive_list.rend(); ++it) {
      frame_id_t f_id = *it;

      auto mit = alive_map_.find(f_id);
      if (mit == alive_map_.end()) {
        continue;
      }

      auto fs = mit->second;
      if (!fs->evictable_) {
        continue;
      }

      // Found evictable frame
      page_id_t p_id = fs->page_id_;

      // Remove from alive list (convert reverse iterator to forward iterator)
      alive_list.erase(std::next(it).base());
      alive_map_.erase(mit);

      --curr_size_;
      // Add to ghost list at front (most recent)
      ghost_list.push_front(p_id);
      auto ghost_status_ptr = std::make_shared<FrameStatus>(p_id, static_cast<frame_id_t>(-1), false, ghost_status);
      ghost_status_ptr->ghost_iter_ = ghost_list.begin();
      ghost_map_[p_id] = ghost_status_ptr;

      return f_id;
    }

    return std::nullopt;
  };

  bool prefer_mru = (mru_.size() >= mru_target_size_);
  std::cout << "eviction: " << (prefer_mru ? "MRU" : "MFU") << " mru_size=" << mru_.size()
            << " target=" << mru_target_size_ << std::endl;

  if (prefer_mru) {
    if (auto res = try_evict(mru_, mru_ghost_, ArcStatus::MRU_GHOST)) {
      std::cout << "Evicted from MRU: " << *res << std::endl;
      return res;
    }
    if (auto res = try_evict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST)) {
      std::cout << "Evicted from MFU (fallback): " << *res << std::endl;
      return res;
    }
  } else {
    if (auto res = try_evict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST)) {
      std::cout << "Evicted from MFU: " << *res << std::endl;
      return res;
    }
    if (auto res = try_evict(mru_, mru_ghost_, ArcStatus::MRU_GHOST)) {
      std::cout << "Evicted from MRU (fallback): " << *res << std::endl;
      return res;
    }
  }

  return std::nullopt;
}

void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);

  // Case 1: Hit in alive lists (MRU or MFU).
  auto it_alive = alive_map_.find(frame_id);
  if (it_alive != alive_map_.end()) {
    auto fs = it_alive->second;

    // Remove from its current alive list.
    if (fs->arc_status_ == ArcStatus::MRU) {
      mru_.erase(fs->iter_);
    } else {
      BUSTUB_ASSERT(fs->arc_status_ == ArcStatus::MFU, "Alive frame must be MRU or MFU");
      mfu_.erase(fs->iter_);
    }

    // Promote to MFU and update page id.
    fs->arc_status_ = ArcStatus::MFU;

    // Insert at front of MFU and store iterator.
    mfu_.push_front(frame_id);
    fs->iter_ = mfu_.begin();
    return;
  }

  // Case 2: Hit in ghost lists
  auto ghost_it = ghost_map_.find(page_id);
  if (ghost_it != ghost_map_.end()) {
    auto gfs = ghost_it->second;
    bool was_b1 = (gfs->arc_status_ == ArcStatus::MRU_GHOST);

    size_t b1 = mru_ghost_.size();
    size_t b2 = mfu_ghost_.size();

    // Adaptation
    if (was_b1) {
      size_t delta = (b1 >= b2 ? 1 : b2 / b1);
      mru_target_size_ = std::min(mru_target_size_ + delta, replacer_size_);
    } else {
      size_t delta = (b2 >= b1 ? 1 : b1 / b2);
      mru_target_size_ = (mru_target_size_ > delta) ? (mru_target_size_ - delta) : 0;
    }

    // Remove from ghost
    if (was_b1) {
      mru_ghost_.erase(gfs->ghost_iter_);
    } else {
      mfu_ghost_.erase(gfs->ghost_iter_);
    }
    ghost_map_.erase(ghost_it);

    // Insert into MFU (T2)
    auto fs = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    mfu_.push_front(frame_id);
    fs->iter_ = mfu_.begin();
    alive_map_[frame_id] = fs;
    return;
  }

  size_t t1 = mru_.size();
  size_t b1 = mru_ghost_.size();

  // Case 4(a)
  if (t1 + b1 == replacer_size_) {
    if (!mru_ghost_.empty()) {
      page_id_t old = mru_ghost_.back();
      mru_ghost_.pop_back();
      ghost_map_.erase(old);
    }
  }
  // Case 4(b)
  else if (mru_.size() + mru_ghost_.size() < replacer_size_) {
    if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() == replacer_size_ * 2) {
      if (!mfu_ghost_.empty()) {
        page_id_t old = mfu_ghost_.back();
        mfu_ghost_.pop_back();
        ghost_map_.erase(old);
      }
    }
  }

  // Insert into MRU (T1)
  auto fs = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  mru_.push_front(frame_id);
  fs->iter_ = mru_.begin();
  alive_map_[frame_id] = fs;
}

void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    throw std::runtime_error("SetEvictable() called on invalid frame_id");
  }

  auto status = alive_map_[frame_id];
  if (status->evictable_ && !set_evictable) {
    curr_size_--;
    status->evictable_ = false;
  } else if (!status->evictable_ && set_evictable) {
    curr_size_++;
    status->evictable_ = true;
  }
}

void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = alive_map_.find(frame_id);
  if (it == alive_map_.end()) {
    return;
  }

  auto fs = it->second;
  if (!fs->evictable_) {
    throw std::runtime_error("Remove() called on non-evictable frame");
  }

  // Remove from its alive list using stored iterator.
  if (fs->arc_status_ == ArcStatus::MFU) {
    mfu_.erase(fs->iter_);
  } else if (fs->arc_status_ == ArcStatus::MRU) {
    mru_.erase(fs->iter_);
  } else {
    // Should not happen for alive entries.
    BUSTUB_ASSERT(false, "Alive frame must be MRU or MFU in Remove()");
  }

  alive_map_.erase(it);
  if (curr_size_ > 0) {
    --curr_size_;
  }
}

auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub