/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
    // you may define your own constructor based on your member variables
    IndexIterator(int offset, B_PLUS_TREE_LEAF_PAGE_TYPE *curr_page,
                  BufferPoolManager *buffer_pool_manager);
    ~IndexIterator();

    bool isEnd();

    const MappingType &operator*();

    IndexIterator &operator++();

private:
    // offset of the current pair in page
    int offset_;
    // current B+ tree leaf page
    B_PLUS_TREE_LEAF_PAGE_TYPE *curr_page_;
    // buffer pool manager to fetch pages
    BufferPoolManager *buffer_pool_manager_;
};

} // namespace cmudb
