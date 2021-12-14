//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.h
//
// Identification: src/include/recovery/log_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <future>              // NOLINT
#include <mutex>               // NOLINT

#include "recovery/log_record.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * LogManager maintains a separate thread that is awakened whenever the log buffer is full or whenever a timeout
 * happens. When the thread is awakened, the log buffer's content is written into the disk log file.
 */
class LogManager {
 public:
  explicit LogManager(DiskManager *disk_manager)
      : promise_(nullptr), flush_lsn_(0), next_lsn_(0), persistent_lsn_(INVALID_LSN),
        offset_(0), disk_manager_(disk_manager) {
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }

  LogManager(LogManager const &) = delete;
  LogManager &operator=(LogManager const &) = delete;

  void RunFlushThread();
  void StopFlushThread();
  // 前端线程调用添加日志
  lsn_t AppendLogRecord(LogRecord *log_record);

  // get/set helper
  inline lsn_t GetNextLSN() { return next_lsn_; }
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }

/* 为实现调用wakeup的线程与日志线程间的同步 */
  inline std::promise<void> *GetPromise() { return promise_; }
  inline void SetPromise(std::promise<void> *p) { promise_ = p; }
  // 由buffer pool manager调用
  void WakeupFlushThread(std::promise<void> *promise);

 private:
  inline void swapBuffer();
  // 用于与buffer pool线程的同步
  std::promise<void> *promise_;

  // last log records in the flush_buffer_;
  // flush_buffer中的最后一条日志
  lsn_t flush_lsn_;
  /** The atomic counter which records the next log sequence number. */
  std::atomic<lsn_t> next_lsn_;
  /** The log records before and including the persistent lsn have been written to disk. */
  std::atomic<lsn_t> persistent_lsn_;
  // log缓冲区
  char *log_buffer_;
  char *flush_buffer_;

  int offset_;

  std::mutex latch_;

  std::thread *flush_thread_ __attribute__((__unused__));

  std::condition_variable cv_;

  DiskManager *disk_manager_ __attribute__((__unused__));
};

}  // namespace bustub
