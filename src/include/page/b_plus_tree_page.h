/**
 * b_plus_tree_page.h
 *
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 20 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "index/generic_key.h"

namespace cmudb {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS                                               \
  template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

// Abstract class.
class BPlusTreePage {
public:
    bool IsLeafPage() const;
    bool IsRootPage() const;
    void SetPageType(IndexPageType page_type);

    int GetSize() const;
    void SetSize(int size);
    void IncreaseSize(int amount);

    int GetMaxSize() const;
    void SetMaxSize(int max_size);
    int GetMinSize() const;

    page_id_t GetParentPageId() const;
    void SetParentPageId(page_id_t parent_page_id);

    page_id_t GetPageId() const;
    void SetPageId(page_id_t page_id);

    void SetLSN(lsn_t lsn = INVALID_LSN);

    template <typename PageType>
    PageType FetchPage(BufferPoolManager* buffer_pool_manager, page_id_t page_id) {
      assert(page_id_ != INVALID_PAGE_ID);
      auto page = buffer_pool_manager->FetchPage(page_id);
      if (page == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX, "All pages are pinned.");
      }
      return reinterpret_cast<PageType>(page->GetData());
    }

private:
    // member variable, attributes that both internal and leaf page share
    IndexPageType page_type_;
    lsn_t lsn_;
    int size_;
    int max_size_;
    page_id_t parent_page_id_;
    page_id_t page_id_;
};

} // namespace cmudb
