/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    SetMaxSize((PAGE_SIZE - sizeof(*this)) / sizeof(MappingType) - 1);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    // first key is invalid
    assert(0 < index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    // first key is invalid
    assert(0 < index < GetSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); ++i) {
        if (array[i].second == value) {
            return i;
        }
    }

    return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(0 <= index < GetSize());
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    assert(GetSize() > 1);
    int start = 1, end = GetSize();

    while (start < end) {
        int mid = start + (end - start) / 2;
        if (comparator(key, KeyAt(mid)) < 0) {
            end = mid;
        } else {
            start = mid + 1;
        }
    }

    return ValueAt(start - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
        const ValueType &old_value, const KeyType &new_key,
        const ValueType &new_value) {
    assert(IsRootPage());
    assert(GetSize() == 0);

    array[0].second = old_value;
    array[1].first = new_key;
    array[1].second = new_value;
    IncreaseSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return: new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
        const ValueType &old_value, const KeyType &new_key,
        const ValueType &new_value) {
    int index = ValueIndex(old_value);
    assert(index >= 0);

    for (int i = GetSize(); i > index + 1; --i) {
        array[i].first = array[i - 1].first;
        array[i].second = array[i - 1].second;
    }
    array[index + 1].first = new_key;
    array[index + 1].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
        BPlusTreeInternalPage *recipient,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() == GetMaxSize() + 1);
    assert(recipient->GetSize() == 0);

    // the first key moved to recipient will be invalid
    int size = GetSize(), half = GetMinSize();
    recipient->CopyHalfFrom(array + half, size - half, buffer_pool_manager);
    SetSize(half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
        MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    for (int i = 0; i < size; ++i) {
        array[i].first = items[i].first;
        array[i].second = items[i].second;
        // update parent page id for all children
        auto child = FetchPage<BPlusTreePage *>(buffer_pool_manager, array[i].second);
        child->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }
    SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    assert(GetSize() > 0);
    assert(0 < index < GetSize());

    for (int i = index; i < GetSize() - 1; ++i) {
        array[i].first = array[i + 1].first;
        array[i].second = array[i + 1].second;
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    assert(GetSize() == 1);
    auto child = ValueAt(0);
    SetSize(0);
    return child;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
        BPlusTreeInternalPage *recipient, int index_in_parent,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetParentPageId() == recipient->GetParentPageId());
    assert(GetSize() < GetMinSize() || recipient->GetSize() < recipient->GetMinSize());
    assert(GetSize() <= GetMinSize() && recipient->GetSize() <= recipient->GetMinSize());

    // demote parent key, place it in array[0].first for transfer
    auto parent = FetchPage<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
        buffer_pool_manager, GetParentPageId());
    array[0].first = parent->KeyAt(index_in_parent);
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
    recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
        MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    for (int i = GetSize(), j = 0; j < size; ++i, ++j) {
        array[i].first = items[j].first;
        array[i].second = items[j].second;
        // update parent page id for all copied children
        auto child = FetchPage<BPlusTreePage *>(buffer_pool_manager, array[i].second);
        child->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
        BPlusTreeInternalPage *recipient, int parent_index,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() > GetMinSize());
    assert(recipient->GetSize() < recipient->GetMinSize());

    auto parent = FetchPage<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
        buffer_pool_manager, GetParentPageId());
    MappingType pair = std::make_pair(parent->KeyAt(parent_index), ValueAt(0));
    recipient->CopyLastFrom(pair, buffer_pool_manager);

    for (int i = 0; i < GetSize() - 1; ++i) {
        array[i].first = array[i + 1].first;
        array[i].second = array[i + 1].second;
    }
    IncreaseSize(-1);

    parent->SetKeyAt(parent_index, KeyAt(0));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
        const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    array[GetSize()] = pair;
    IncreaseSize(1);

    // update parent page id of the copied child
    auto child = FetchPage<BPlusTreePage *>(
        buffer_pool_manager, ValueAt(GetSize() - 1));
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
        BPlusTreeInternalPage *recipient, int parent_index,
        BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() > GetMinSize());
    assert(recipient->GetSize() < recipient->GetMinSize());

    auto parent = FetchPage<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(
        buffer_pool_manager, parent_index);
    MappingType pair = std::make_pair(
        parent->KeyAt(parent_index), ValueAt(GetSize() - 1));
    recipient->CopyFirstFrom(pair, buffer_pool_manager);
    IncreaseSize(-1);

    parent->SetKeyAt(parent_index, KeyAt(GetSize() - 1));
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
        const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    for (int i = GetSize(); i > 0; --i) {
        array[i].first = array[i - 1].first;
        array[i].second = array[i - 1].second;
    }
    array[1].first = pair.first;
    array[0].second = pair.second;
    IncreaseSize(1);

    // update parent page id of the copied child
    auto child = FetchPage<BPlusTreePage *>(
        buffer_pool_manager, ValueAt(0));
    child->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
        std::queue<BPlusTreePage *> *queue,
        BufferPoolManager *buffer_pool_manager) {
    for (int i = 0; i < GetSize(); i++) {
        auto *page = buffer_pool_manager->FetchPage(array[i].second);
        if (page == nullptr)
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        BPlusTreePage *node =
                reinterpret_cast<BPlusTreePage *>(page->GetData());
        queue->push(node);
    }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
    if (GetSize() == 0) {
        return "";
    }
    std::ostringstream os;
    if (verbose) {
        os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
    }

    int entry = verbose ? 0 : 1;
    int end = GetSize();
    bool first = true;
    while (entry < end) {
        if (first) {
            first = false;
        } else {
            os << " ";
        }
        os << std::dec << array[entry].first.ToString();
        if (verbose) {
            os << "(" << array[entry].second << ")";
        }
        ++entry;
    }
    return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
} // namespace cmudb
