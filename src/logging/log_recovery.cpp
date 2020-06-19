/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {

/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                       LogRecord &log_record) {
    if (offset_ + LogRecord::HEADER_SIZE > LOG_BUFFER_SIZE) {
        return false;
    }

    int size = *reinterpret_cast<const int32_t *>(data);
    if (size <= 0 || offset_ + size > LOG_BUFFER_SIZE) {
        return false;
    }

    log_record.size_ = size;
    log_record.lsn_ = *reinterpret_cast<const lsn_t *>(data + 4);
    log_record.txn_id_ = *reinterpret_cast<const txn_id_t *>(data + 8);
    log_record.prev_lsn_ = *reinterpret_cast<const lsn_t *>(data + 12);
    log_record.log_record_type_ = *reinterpret_cast<const LogRecordType *>(data + 16);
    data += LogRecord::HEADER_SIZE;

    int rid_size = sizeof(RID);
    switch (log_record.log_record_type_) {
        case LogRecordType::INSERT:
            log_record.insert_rid_ = *reinterpret_cast<const RID *>(data);
            log_record.insert_tuple_.DeserializeFrom(data + rid_size);
            break;
        case LogRecordType::UPDATE:
            log_record.update_rid_ = *reinterpret_cast<const RID *>(data);
            log_record.old_tuple_.DeserializeFrom(data + rid_size);
            log_record.new_tuple_.DeserializeFrom(data + rid_size +
                sizeof(int32_t) + log_record.old_tuple_.GetLength());
            break;
        case LogRecordType::NEWPAGE:
            log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(data);
            break;
        case LogRecordType::APPLYDELETE:
        case LogRecordType::MARKDELETE:
        case LogRecordType::ROLLBACKDELETE:
            log_record.delete_rid_ = *reinterpret_cast<const RID *>(data);
            log_record.delete_tuple_.DeserializeFrom(data + rid_size);
            break;
        default:
            break;
    }

    return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
    assert(ENABLE_LOGGING == false);
    int read_offset = 0;
    active_txn_.clear();
    lsn_mapping_.clear();

    while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, read_offset)) {
        offset_ = 0;
        LogRecord record;
        while (DeserializeLogRecord(log_buffer_ + offset_, record)) {
            if (record.log_record_type_ == LogRecordType::COMMIT ||
                record.log_record_type_ == LogRecordType::ABORT) {
                active_txn_.erase(record.txn_id_);
            } else {
                active_txn_[record.txn_id_] = record.lsn_;
            }

            if (record.log_record_type_ == LogRecordType::INSERT) {
                page_id_t page_id = record.insert_rid_.GetPageId();
                auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
                bool redo = record.lsn_ > page->GetLSN();
                if (redo) {
                    page->InsertTuple(record.insert_tuple_, record.insert_rid_, nullptr, nullptr, nullptr);
                }
                buffer_pool_manager_->UnpinPage(page_id, redo);
            } else if (record.log_record_type_ == LogRecordType::UPDATE) {
                page_id_t page_id = record.update_rid_.GetPageId();
                auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
                bool redo = record.lsn_ > page->GetLSN();
                if (record.lsn_ > page->GetLSN()) {
                    page->UpdateTuple(
                        record.new_tuple_, record.old_tuple_, record.update_rid_, nullptr, nullptr, nullptr);
                }
                buffer_pool_manager_->UnpinPage(page_id, redo);
            } else if (record.log_record_type_ == LogRecordType::APPLYDELETE) {
                page_id_t page_id = record.delete_rid_.GetPageId();
                auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
                bool redo = record.lsn_ > page->GetLSN();
                if (redo) {
                    page->ApplyDelete(record.delete_rid_, nullptr, nullptr);
                }
                buffer_pool_manager_->UnpinPage(page_id, redo);
            } else if (record.log_record_type_ == LogRecordType::MARKDELETE) {
                page_id_t page_id = record.delete_rid_.GetPageId();
                auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
                bool redo = record.lsn_ > page->GetLSN();
                if (redo) {
                    page->MarkDelete(record.delete_rid_, nullptr, nullptr, nullptr);
                }
                buffer_pool_manager_->UnpinPage(page_id, redo);
            } else if (record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                page_id_t page_id = record.delete_rid_.GetPageId();
                auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
                bool redo = record.lsn_ > page->GetLSN();
                if (redo) {
                    page->RollbackDelete(record.delete_rid_, nullptr, nullptr);
                }
                buffer_pool_manager_->UnpinPage(page_id, redo);
            } else if (record.log_record_type_ == LogRecordType::NEWPAGE) {
                page_id_t prev_page_id = record.prev_page_id_;
                page_id_t new_page_id;
                auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
                page->Init(new_page_id, PAGE_SIZE, prev_page_id, nullptr, nullptr);
                if (prev_page_id != INVALID_PAGE_ID) {
                    auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
                    bool redo = prev_page->GetNextPageId() == INVALID_PAGE_ID;
                    if (redo) {
                        prev_page->SetNextPageId(new_page_id);
                    } else {
                        assert(new_page_id == prev_page->GetNextPageId());
                    }
                    buffer_pool_manager_->UnpinPage(prev_page_id, redo);
                }
                buffer_pool_manager_->UnpinPage(new_page_id, true);
            }

            lsn_mapping_[record.lsn_] = read_offset;
            read_offset += record.size_;
            offset_ += record.size_;
        }
    }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
    assert(ENABLE_LOGGING == false);
    offset_ = 0;
    int next_offset;

    for (auto entry : active_txn_) {
        next_offset = lsn_mapping_[entry.second];
        while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, next_offset)) {
            LogRecord record;
            assert(DeserializeLogRecord(log_buffer_, record));

            if (record.log_record_type_ == LogRecordType::BEGIN) {
                break;
            }

            page_id_t page_id = record.insert_rid_.GetPageId();
            auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
            if (record.log_record_type_ == LogRecordType::INSERT) {
                page->ApplyDelete(record.insert_rid_, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, true);
            } else if (record.log_record_type_ == LogRecordType::UPDATE) {
                page->UpdateTuple(record.old_tuple_, record.new_tuple_, record.update_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, true);
            } else if (record.log_record_type_ == LogRecordType::APPLYDELETE) {
                page->InsertTuple(record.delete_tuple_, record.delete_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, true);
            } else if (record.log_record_type_ == LogRecordType::MARKDELETE) {
                page->RollbackDelete(record.delete_rid_, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, true);
            } else if (record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
                page->MarkDelete(record.delete_rid_, nullptr, nullptr, nullptr);
                buffer_pool_manager_->UnpinPage(page_id, true);
            } else {
                throw Exception("unexpected log record type");
            }
            next_offset = lsn_mapping_[record.prev_lsn_];
        }
    }

    active_txn_.clear();
    lsn_mapping_.clear();
}

} // namespace cmudb
