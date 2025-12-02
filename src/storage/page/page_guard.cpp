#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "buffer/buffer_pool_manager.h"
#include "common/macros.h"

namespace bustub {

/**********************************************************************************************************************/
/* ReadPageGuard Implementation */
/**********************************************************************************************************************/

ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // Acquire read latch to protect against concurrent writes
  // NOTE: Pin count already incremented by BufferPoolManager
  frame_->rwlatch_.lock_shared();

  // Mark this guard as valid
  is_valid_ = true;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // Transfer ownership from 'that' to 'this'
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  // Invalidate 'that' to prevent double-free
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  that.is_valid_ = false;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // First, drop current resources
    Drop();

    // Transfer ownership from 'that' to 'this'
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    // Invalidate 'that' to prevent double-free
    that.page_id_ = INVALID_PAGE_ID;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
    that.is_valid_ = false;
  }
  return *this;
}

auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

void ReadPageGuard::Flush() {
  if (!is_valid_) {
    return;
  }
  
  // Check if the page is dirty (shouldn't be for read guard, but could be from previous write)
  if (!frame_->is_dirty_) {
    return;  // Nothing to flush
  }

  // Schedule disk write
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest req{};
  req.is_write_ = true;
  req.data_ = frame_->GetDataMut();
  req.page_id_ = page_id_;
  req.callback_ = std::move(promise);
  std::vector<DiskRequest> batch;
  batch.push_back(std::move(req));
  disk_scheduler_->Schedule(batch);
  future.get();

  // Mark as clean after successful flush
  frame_->is_dirty_ = false;
}

void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;  // Already dropped, nothing to do
  }

  // Mark as invalid first to prevent double-drop
  is_valid_ = false;

  // Release the read latch (must do this BEFORE taking BPM latch)
  if (frame_ != nullptr) {
    frame_->rwlatch_.unlock_shared();
  }

  // Take the buffer pool manager's latch to safely modify shared state
  if (bpm_latch_ != nullptr && frame_ != nullptr && replacer_ != nullptr) {
    std::lock_guard<std::mutex> lock(*bpm_latch_);

    // Decrement pin count
    frame_->pin_count_.fetch_sub(1);

    // If pin count reaches 0, mark frame as evictable
    if (frame_->pin_count_.load() == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }

  // Clear all pointers
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/* WritePageGuard Implementation */
/**********************************************************************************************************************/

WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock();

  // CRITICAL: Mark as dirty when acquiring write access
  frame_->is_dirty_ = true;

  is_valid_ = true;
}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // Transfer ownership from 'that' to 'this'
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  // Invalidate 'that' to prevent double-free
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  that.is_valid_ = false;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    // First, drop current resources
    Drop();

    // Transfer ownership from 'that' to 'this'
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    // Invalidate 'that' to prevent double-free
    that.page_id_ = INVALID_PAGE_ID;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
    that.is_valid_ = false;
  }
  return *this;
}

auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

void WritePageGuard::Flush() {
  if (!is_valid_) {
    return;
  }

  // Only flush if dirty
  if (!frame_->is_dirty_) {
    return;
  }

  // Schedule a write to disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest req{};
  req.is_write_ = true;
  req.data_ = frame_->GetDataMut();
  req.page_id_ = page_id_;
  req.callback_ = std::move(promise);
  std::vector<DiskRequest> batch;
  batch.push_back(std::move(req));
  disk_scheduler_->Schedule(batch);
  future.get();

  // Mark as clean after successful flush
  frame_->is_dirty_ = false;
}

void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  is_valid_ = false;

  // Release the write latch FIRST
  if (frame_ != nullptr) {
    frame_->rwlatch_.unlock();
  }

  // Then update BPM state
  if (bpm_latch_ != nullptr && frame_ != nullptr && replacer_ != nullptr) {
    std::lock_guard<std::mutex> lock(*bpm_latch_);
    frame_->pin_count_.fetch_sub(1);
    if (frame_->pin_count_.load() == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }

  // Clear pointers
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub