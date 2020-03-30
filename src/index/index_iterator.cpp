/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int offset,
                                  B_PLUS_TREE_LEAF_PAGE_TYPE *curr_page,
                                  BufferPoolManager *buffer_pool_manager)
    : offset_(offset), curr_page_(curr_page),
      buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    buffer_pool_manager_->UnpinPage(curr_page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
    return curr_page_->GetNextPageId() == INVALID_PAGE_ID
           && offset_ >= curr_page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
    assert(!isEnd());
    return curr_page_->GetItem(offset_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    assert(!isEnd());

    if (++offset_ >= curr_page_->GetSize()) {
        page_id_t next_page_id = curr_page_->GetNextPageId();
        // if already at the end of last page, just return
        if (next_page_id == INVALID_PAGE_ID) {
            return *this;
        }
        buffer_pool_manager_->UnpinPage(curr_page_->GetPageId(), false);
        curr_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(
            buffer_pool_manager_->FetchPage(next_page_id));
        assert(curr_page_ != nullptr);
        offset_ = 0;
    }

    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
