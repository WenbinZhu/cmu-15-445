/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <mutex>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
    // constructor
    ExtendibleHash(size_t size);
    // helper function to generate hash addressing
    size_t HashKey(const K &key);
    // helper function to get global & local depth
    int GetGlobalDepth() const;
    int GetLocalDepth(int bucket_id) const;
    int GetNumBuckets() const;
    // lookup and modifier
    bool Find(const K &key, V &value) override;
    bool Remove(const K &key) override;
    void Insert(const K &key, const V &value) override;

private:
    // get the index of the bucket by key
    int GetBucketIndex(const K &key);

    // hash bucket storing key value pairs
    class Bucket {
    public:
        // local depth of the bucket
        int local_depth;
        // slots to store key value pairs
        std::vector<std::pair<K, V>> slots;
        // constructor
        Bucket(int depth) : local_depth(depth) {}
    };

    // mutex to protect critical sections
    std::mutex mutex;
    // global depth of the hash table
    int global_depth;
    // size of each bucket
    const size_t bucket_size;
    // bucket directory with size power two of global depth
    std::vector<std::shared_ptr<Bucket>> directory;
};
} // namespace cmudb
