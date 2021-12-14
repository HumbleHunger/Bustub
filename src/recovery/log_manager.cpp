//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
	if (!enable_logging) {
		enable_logging = true;

		flush_thread_ = new std::thread([&]() {
			while (enable_logging) {
				std::unique_lock<std::mutex> lock(latch_);
				// 如果等待超时了并且缓冲区有数据则交换缓冲区
				if (cv_.wait_for(lock, log_timeout) == std::cv_status::timeout && offset_ != 0) {
					swapBuffer();
				}
				// 保存缓冲区最后一条日志的lsn，减小互斥区的长度
				lsn_t lsn = flush_lsn_;
				lock.unlock();
				// buffer pool没有在刷新日志
				if (enable_logging && !disk_manager_->GetFlushState() && lsn > persistent_lsn_) {
					// 将日志刷新到磁盘
					disk_manager_->WriteLog(flush_buffer_, LOG_BUFFER_SIZE);
					// 更新持久化日志的顺序号
					SetPersistentLSN(lsn);
					// 写入日志完毕，如果是被buffer pool调用则进行通知
					if (promise_ != nullptr) {
						promise_->set_value();
					}
				}
			}
		});
	}
}


/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
	if (enable_logging) {
    std::cout << "stop log thread" << std::endl;
		enable_logging = false;

		cv_.notify_one();

		std::unique_lock<std::mutex> lock(latch_);
		if (flush_thread_ && flush_thread_->joinable()) {
      lock.unlock();
      flush_thread_->join();
    }
    // log线程结束后delete掉new的空间
    delete flush_thread_;
	}
}

inline void LogManager::swapBuffer() {
  char *tmp = log_buffer_;
  log_buffer_ = flush_buffer_;
  flush_buffer_ = tmp;

  offset_ = 0;
  flush_lsn_ = next_lsn_ - 1;
}

/*
 * wake up flush thread, only called by buffer pool manager
 * when it wants to force flush
 */
void LogManager::WakeupFlushThread(std::promise<void> *promise) {
  {
    std::lock_guard<std::mutex> lock(latch_);
    swapBuffer();
    SetPromise(promise);
  }

  // wake up flush thread
  cv_.notify_one();

  // waiting for flush done
  if (promise != nullptr) {
    promise->get_future().wait();
  }
  SetPromise(nullptr);
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
	std::lock_guard<std::mutex> lock(latch_);

	if (offset_ + log_record->size_ > LOG_BUFFER_SIZE) {
		swapBuffer();
		cv_.notify_one();
	}

	log_record->lsn_ = next_lsn_++;

  std::cout << offset_ << std::endl;
	// for begin/commit/abort, we are done
  memcpy(log_buffer_ + offset_, log_record, LogRecord::HEADER_SIZE);
  int pos = offset_ + LogRecord::HEADER_SIZE;

	  if (log_record->log_record_type_ == LogRecordType::INSERT) {
    // for insert
    memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);

  } else if (log_record->log_record_type_ == LogRecordType::MARKDELETE ||
      log_record->log_record_type_ == LogRecordType::ROLLBACKDELETE ||
      log_record->log_record_type_ == LogRecordType::APPLYDELETE) {

    // for delete
    memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);

  } else if (log_record->log_record_type_ == LogRecordType::UPDATE) {
    // for update
    memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += log_record->old_tuple_.GetLength();
    log_record->new_tuple_.SerializeTo(log_buffer_ + pos);

  } else if (log_record->log_record_type_ == LogRecordType::NEWPAGE) {
    // for new page
    memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
  }

	offset_ += log_record->size_;
	return log_record->lsn_;
}

}  // namespace bustub
