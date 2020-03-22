/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
        : index_name_(name), root_page_id_(root_page_id),
          buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    if (IsEmpty()) {
        return false;
    }

    result.resize(1);
    auto leaf_page = FindLeafPage(key);
    bool exists = leaf_page->Lookup(key, result[0], comparator_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    return exists;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    if (IsEmpty()) {
        StartNewTree(key, value);
        return true;
    }

    return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    Page *new_page = buffer_pool_manager_->NewPage(root_page_id_);
    if (new_page == nullptr) {
        throw Exception("failed to allocate new page");
    }

    auto root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID);
    root_node->Insert(key, value, comparator_);

    UpdateRootPageId(true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    auto leaf_page = FindLeafPage(key);
    int old_size = leaf_page->GetSize();
    int new_size = leaf_page->Insert(key, value, comparator_);
    bool inserted = new_size != old_size;

    if (new_size > leaf_page->GetMaxSize()) {
        auto new_node = Split(leaf_page);
        // copy the first key in the new node to parent
        InsertIntoParent(leaf_page, new_node->KeyAt(0), new_node, transaction);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), inserted);

    return inserted;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr) {
        throw Exception("failed to allocate new page");
    }

    auto new_node = reinterpret_cast<N *>(new_page->GetData());
    new_node->Init(new_page_id, node->GetParentPageId());
    node->MoveHalfTo(new_node, buffer_pool_manager_);

    return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    page_id_t parent_page_id = old_node->GetParentPageId();

    if (parent_page_id == INVALID_PAGE_ID) {
        page_id_t new_page_id;
        Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
        if (new_page == nullptr) {
            throw Exception("failed to allocate new page");
        }
        auto new_root = reinterpret_cast<BPLUSTREE_INTERNAL_TYPE *>(new_page->GetData());
        new_root->Init(new_page_id, INVALID_PAGE_ID);
        new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        root_page_id_ = new_page_id;
        UpdateRootPageId(false);
        old_node->SetParentPageId(new_root->GetPageId());
        new_node->SetParentPageId(new_root->GetPageId());
    } else {
        auto parent_node = FetchPage<BPLUSTREE_INTERNAL_TYPE *>(parent_page_id);
        int size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if (size > parent_node->GetMaxSize()) {
            auto next_node = Split(parent_node);
            // promote first key in the new node, first key is invalid in new node
            InsertIntoParent(parent_node, next_node->KeyAt(0), next_node, transaction);
            buffer_pool_manager_->UnpinPage(next_node->GetPageId(), true);
        }
        buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    if (IsEmpty()) {
        return;
    }

    auto leaf_page = FindLeafPage(key);
    int old_size = leaf_page->GetSize();
    int new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
    bool removed = new_size != old_size;

    if (removed && new_size < leaf_page->GetMinSize()) {
        if (CoalesceOrRedistribute(leaf_page, transaction)) {
            buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
            buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
            return;
        }
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), removed);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    if (node->IsRootPage()) {
        return AdjustRoot(node);
    }

    page_id_t parent_page_id = node->GetParentPageId();
    auto parent_node = FetchPage<BPLUSTREE_INTERNAL_TYPE *>(parent_page_id);
    int node_index = parent_node->ValueIndex(node->GetPageId());

    int sibling_index = node_index == 0 ? 1 : node_index - 1;
    page_id_t sibling_page_id = parent_node->ValueAt(sibling_index);
    auto sibling_node = FetchPage<N *>(sibling_page_id);
    bool coalesce = sibling_node->GetSize() <= sibling_node->GetMinSize();

    if (coalesce) {
        bool delete_parent = Coalesce(
            sibling_node, node, parent_node, node_index, transaction);
        buffer_pool_manager_->UnpinPage(sibling_page_id, true);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
        // if node index is 0, node and sibling is swaped, so sibling
        // paged need to be deleted instead of this node
        if (node_index == 0) {
            buffer_pool_manager_->DeletePage(sibling_page_id);
        }
        if (delete_parent) {
            buffer_pool_manager_->DeletePage(parent_page_id);
        }
    } else {
        Redistribute(sibling_node, node, node_index);
        buffer_pool_manager_->UnpinPage(sibling_page_id, true);
        buffer_pool_manager_->UnpinPage(parent_page_id, true);
    }

    return coalesce && node_index != 0;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
        N *&neighbor_node, N *&node,
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
        int index, Transaction *transaction) {
    if (index == 0) {
        // swapping pointer will not afftect caller, however
        // swap(*node, *neighbor_node) does affect caller
        std::swap(node, neighbor_node);
        index = 1;
    }

    // in case of internal page, demotion of separator key is
    // done in MoveAllTo, for leaf page, it is simply deleted
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(index);

    if (parent->GetSize() < parent->GetMinSize()) {
        return CoalesceOrRedistribute(parent, transaction);
    }

    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    if (index == 0) {
        neighbor_node->MoveFirstToEndOf(node, 1, buffer_pool_manager_);
    } else {
        neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
    }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    // root node is the last element in the whole tree
    if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
        root_page_id_ = INVALID_PAGE_ID;
        UpdateRootPageId(false);
        return true;
    }

    // root node is an internal node and has no valid key but one child
    if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
        auto internal_node = reinterpret_cast<BPLUSTREE_INTERNAL_TYPE *>(old_root_node);
        page_id_t new_root_pid = internal_node->RemoveAndReturnOnlyChild();
        auto new_root_node = FetchPage<BPlusTreePage *>(new_root_pid);
        new_root_node->SetParentPageId(INVALID_PAGE_ID);
        root_page_id_ = new_root_pid;
        UpdateRootPageId(false);
        buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
        return true;
    }

    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
    return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
    assert(!IsEmpty());

    auto curr_page = FetchPage<BPlusTreePage *>(root_page_id_);
    while (!curr_page->IsLeafPage()) {
        auto internal_page = reinterpret_cast<BPLUSTREE_INTERNAL_TYPE *>(curr_page);
        page_id_t next_page_id = leftMost ? internal_page->ValueAt(0)
                                          : internal_page->Lookup(key, comparator_);
        buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
        curr_page = FetchPage<BPlusTreePage *>(next_page_id);
    }

    return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(curr_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
    HeaderPage *header_page = static_cast<HeaderPage *>(
            buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
    if (insert_record) {
        // create a new record<index_name + root_page_id> in header_page
        header_page->InsertRecord(index_name_, root_page_id_);
    } else {
        // update root_page_id in header_page
        header_page->UpdateRecord(index_name_, root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename PageType>
PageType BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
    assert(page_id != INVALID_PAGE_ID);
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    assert(page != nullptr);
    return reinterpret_cast<PageType>(page->GetData());
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
    if (IsEmpty()) {
        return "empty tree\n";
    }

    std::ostringstream os;
    std::queue<BPlusTreePage *> queue0;
    std::queue<BPlusTreePage *> queue1;
    auto curr_rank = &queue0;
    auto next_rank = &queue1;

    curr_rank->push(FetchPage<BPlusTreePage *>(root_page_id_));

    while (!curr_rank->empty()) {
        while (!curr_rank->empty()) {
            auto btree_page = curr_rank->front();
            if (btree_page->IsLeafPage()) {
                auto leaf_page = reinterpret_cast
                    <B_PLUS_TREE_LEAF_PAGE_TYPE *>(btree_page);
                os << leaf_page->ToString(verbose) << "\n";
            } else {
                auto internal_page = reinterpret_cast
                    <BPLUSTREE_INTERNAL_TYPE *>(btree_page);
                os << internal_page->ToString(verbose) << "\n";
                internal_page->QueueUpChildren(next_rank, buffer_pool_manager_);
            }
            curr_rank->pop();
            buffer_pool_manager_->UnpinPage(btree_page->GetPageId(), false);
        }
        std::swap(curr_rank, next_rank);
        os << "=============================================\n";
    }

    return os.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
        input >> key;

        KeyType index_key;
        index_key.SetFromInteger(key);
        RID rid(key);
        Insert(index_key, rid, transaction);
    }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
    int64_t key;
    std::ifstream input(file_name);
    while (input) {
        input >> key;
        KeyType index_key;
        index_key.SetFromInteger(key);
        Remove(index_key, transaction);
    }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
