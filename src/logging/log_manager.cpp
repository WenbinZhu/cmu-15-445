/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {

/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
    ENABLE_LOGGING = true;
    flush_thread_ = new std::thread(&LogManager::FlushLog, this);
}

/**
 * flsuh log buffer to disk
 */
void LogManager::FlushLog() {
    while (ENABLE_LOGGING) {
        std::unique_lock<std::mutex> lock(latch_);
        cv_.wait_for(lock, LOG_TIMEOUT);
        int last_lsn = next_lsn_ - 1;
        int flush_size = SwapBuffer();
        std::promise<void> promise;
        flush_future_ = promise.get_future().share();
        // unlock latch before disk write
        lock.unlock();
        disk_manager_->WriteLog(flush_buffer_, flush_size);
        lock.lock();
        SetPersistentLSN(last_lsn);
        promise.set_value();
    }
}

/**
 * swap log buffer and flush buffer
 */
int LogManager::SwapBuffer() {
    std::swap(log_buffer_, flush_buffer_);
    int flush_size = offset_;
    offset_ = 0;

    return flush_size;
}

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
    ENABLE_LOGGING = false;
    std::unique_lock<std::mutex> lock(latch_);
    cv_.notify_one();
    lock.unlock();

    if (flush_thread_ && flush_thread_->joinable()) {
        flush_thread_->join();
    }
    flush_thread_ = nullptr;
}

/*
 * Force and wait for log flush to complete
 */
void LogManager::ForceLogFlushAndWait() {
    std::unique_lock<std::mutex> lock(latch_);
    cv_.notify_one();

    std::shared_future<void> future = flush_future_;
    if (future.valid()) {
        future.wait();
    }
}

/*
 * Wait for async log flush to complete
 */
void LogManager::WaitForLogFlush() {
    std::shared_future<void> future = flush_future_;
    if (future.valid()) {
        future.wait();
    }
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
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
    std::unique_lock<std::mutex> lock(latch_);

    while (offset_ + log_record.size_ > LOG_BUFFER_SIZE) {
        cv_.notify_one();
        lock.unlock();
        std::shared_future<void> future = flush_future_;
        if (future.valid()) {
            future.wait();
        }
        lock.lock();
    }

    size_t rid_size = sizeof(RID);
    log_record.lsn_ = next_lsn_++;
    memcpy(log_buffer_ + offset_, &log_record, log_record.HEADER_SIZE);
    int pos = offset_ + log_record.HEADER_SIZE;

    switch (log_record.log_record_type_) {
        case LogRecordType::INSERT:
            memcpy(log_buffer_ + pos, &log_record.insert_rid_, rid_size);
            pos += rid_size;
            log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
            break;
        case LogRecordType::UPDATE:
            memcpy(log_buffer_ + pos, &log_record.update_rid_, rid_size);
            pos += rid_size;
            log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
            pos += sizeof(int32_t) + log_record.old_tuple_.GetLength();
            log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
            break;
        case LogRecordType::NEWPAGE:
            memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(page_id_t));
            break;
        case LogRecordType::APPLYDELETE:
        case LogRecordType::MARKDELETE:
        case LogRecordType::ROLLBACKDELETE:
            memcpy(log_buffer_ + pos, &log_record.delete_rid_, rid_size);
            pos += rid_size;
            log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
            break;
        default:
            // BEGIN/COMMIT/ABORT record only has header
            break;
    }
    offset_ += log_record.size_;

    return log_record.lsn_;
}

} // namespace cmudb
