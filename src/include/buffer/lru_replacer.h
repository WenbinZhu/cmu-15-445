/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <mutex>
#include <list>
#include <iterator>
#include <unordered_map>
#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace cmudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
    // do not change public interface
    LRUReplacer();

    ~LRUReplacer();

    void Insert(const T &value);

    bool Victim(T &value);

    bool Erase(const T &value);

    size_t Size();

private:
    // mutex to protect critical sections
    std::mutex mutex;
    // linked list to keep track of insertion order
    std::list<T> access_list;
    // hash map to track value positions in linked list
    std::unordered_map<T, typename std::list<T>::iterator> value_map;
};

} // namespace cmudb
