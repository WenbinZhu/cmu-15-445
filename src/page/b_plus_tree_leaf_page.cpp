/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    SetNextPageId(INVALID_PAGE_ID);
    SetMaxSize((PAGE_SIZE - sizeof(*this)) / sizeof(MappingType) - 1);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
    return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
        const KeyType &key, const KeyComparator &comparator) const {
    int start = 0, end = GetSize();

    // if not exists, return the index after the last one
    while (start < end) {
        int mid = start + (end - start) / 2;
        if (comparator(key, array[mid].first) > 0) {
            start = mid + 1;
        } else {
            end = mid;
        }
    }

    return start;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
    assert(index >= 0 && index < GetSize());
    return array[index].first;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const {
    assert(index >= 0 && index < GetSize());
    return array[index].second;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
    assert(index >= 0 && index < GetSize());
    return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
    assert(GetSize() <= GetMaxSize());

    // if same key found, do not perform insertion
    int index = KeyIndex(key, comparator);
    if (index < GetSize() && !comparator(key, KeyAt(index))) {
        return GetSize();
    }

    for (int i = GetSize(); i > index; --i) {
        array[i].first = array[i - 1].first;
        array[i].second = array[i - 1].second;
    }
    array[index].first = key;
    array[index].second = value;
    IncreaseSize(1);

    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
        BPlusTreeLeafPage *recipient,
        __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() == GetMaxSize() + 1);
    assert(recipient->GetSize() == 0);

    int size = GetSize(), half = GetMinSize();
    recipient->CopyHalfFrom(array + size - half, half);
    SetSize(size - half);
    recipient->SetNextPageId(GetNextPageId());
    SetNextPageId(recipient->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
    for (int i = 0; i < size; ++i) {
        array[i].first = items[i].first;
        array[i].second = items[i].second;
    }
    SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
    int index = KeyIndex(key, comparator);
    if (index < GetSize() && !comparator(key, KeyAt(index))) {
        value = ValueAt(index);
        return true;
    }

    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
        const KeyType &key, const KeyComparator &comparator) {
    assert(GetSize() > 0);

    int index = KeyIndex(key, comparator);
    if (index < GetSize() && !comparator(key, KeyAt(index))) {
        for (int i = index + 1; i < GetSize(); ++i) {
            array[i - 1].first = array[i].first;
            array[i - 1].second = array[i].second;
        }
        IncreaseSize(-1);
    }

    return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
    assert(GetSize() <= GetMinSize() && recipient->GetSize() <= recipient->GetMinSize());

    recipient->CopyAllFrom(array, GetSize());
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
    for (int i = GetSize(), j = 0; j < size; ++i, ++j) {
        array[i].first = items[j].first;
        array[i].second = items[j].second;
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
        BPlusTreeLeafPage *recipient, int parent_index,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() > GetMinSize());
    assert(recipient->GetSize() < recipient->GetMinSize());

    recipient->CopyLastFrom(GetItem(0));
    for (int i = 0; i < GetSize() - 1; ++i) {
        array[i].first = array[i + 1].first;
        array[i].second = array[i + 1].second;
    }
    IncreaseSize(-1);

    // update parent page key to first key in this page after move
    auto parent = FetchPage<B_PLUS_TREE_LEAF_PARENT_PAGE_TYPE *>(
        buffer_pool_manager, GetParentPageId());
    parent->SetKeyAt(parent_index, KeyAt(0));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array[GetSize()].first = item.first;
    array[GetSize()].second = item.second;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
        BPlusTreeLeafPage *recipient, int parent_index,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() > GetMinSize());
    assert(recipient->GetSize() < recipient->GetMinSize());

    recipient->CopyFirstFrom(GetItem(GetSize() - 1));
    IncreaseSize(-1);

    // update parent page key to first key in recipient after move
    auto parent = FetchPage<B_PLUS_TREE_LEAF_PARENT_PAGE_TYPE *>(
        buffer_pool_manager, GetParentPageId());
    parent->SetKeyAt(parent_index, recipient->KeyAt(0));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
    for (int i = GetSize(); i > 0; --i) {
        array[i].first = array[i - 1].first;
        array[i].second = array[i - 1].second;
    }
    array[0].first = item.first;
    array[0].second = item.second;
    IncreaseSize(1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
    if (GetSize() == 0) {
        return "";
    }
    std::ostringstream stream;
    if (verbose) {
        stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
               << "]<" << GetSize() << "> ";
    }
    int entry = 0;
    int end = GetSize();
    bool first = true;

    while (entry < end) {
        if (first) {
            first = false;
        } else {
            stream << " ";
        }
        stream << std::dec << array[entry].first;
        if (verbose) {
            stream << "(" << array[entry].second << ")";
        }
        ++entry;
    }
    return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace cmudb
