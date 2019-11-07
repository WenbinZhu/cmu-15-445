#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : global_depth(0), bucket_size(size) {
    directory.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    return global_depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    assert(bucket_id >= 0 && bucket_id < (int) directory.size());
    return directory.at(bucket_id)->local_depth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    return static_cast<int>(directory.size());
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::lock_guard<std::mutex> guard(mutex);

    // find the bucket by key
    int bucket_id = GetBucketIndex(key);
    auto bucket = directory.at(bucket_id);

    // iterate over the bucket slots to find a matching key
    for (auto it : bucket->slots) {
        if (it.first == key) {
            value = it.second;
            return true;
        }
    }

    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
    std::lock_guard<std::mutex> guard(mutex);

    // find the bucket by key
    int bucket_id = GetBucketIndex(key);
    auto bucket = directory.at(bucket_id);
    auto &slots = bucket->slots;

    // remove the pair from the bucket if key exists
    for (auto it = bucket->slots.begin(); it != bucket->slots.end(); ++it) {
        if (it->first == key) {
            slots.erase(it);
            return true;
        }
    }

    return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
    std::lock_guard<std::mutex> guard(mutex);

    // find the bucket by key
    int bucket_id = GetBucketIndex(key);
    auto bucket = directory.at(bucket_id);

    // replace the value if the key exists
    for (auto & it : bucket->slots) {
        if (it.first == key) {
            it.second = value;
            return;
        }
    }

    // whether the bucket needs to split
    // NOTE: while is needed in case the bucket is still full after split
    while (bucket->slots.size() >= bucket_size) {
        // whether the directory needs to expand
        assert(bucket->local_depth <= global_depth);
        if (bucket->local_depth == global_depth) {
            // increase global depth and expand directory
            global_depth++;
            size_t size = directory.size();
            for (size_t i = 0; i < size; ++i) {
                directory.push_back(directory.at(i));
            }
        }
        // increase local depth and split the old bucket
        int local_depth = bucket->local_depth + 1;
        int mask = 1 << (local_depth - 1);
        auto bucket0 = std::make_shared<Bucket>(local_depth);
        auto bucket1 = std::make_shared<Bucket>(local_depth);
        for (auto it : bucket->slots) {
            if (HashKey(it.first) & mask) {
                bucket1->slots.push_back(it);
            } else {
                bucket0->slots.push_back(it);
            }
        }
        // update the directory pointing to new buckets
        for (size_t i = HashKey(key) & (mask - 1); i < directory.size(); i += mask) {
            directory[i] = (i & mask) ? bucket1 : bucket0;
        }

        // update bucket reference to check if bucket is still full
        bucket_id = GetBucketIndex(key);
        bucket = directory.at(bucket_id);
    }

    bucket->slots.push_back(std::make_pair(key, value));
}

/*
 * get the index of the bucket by key
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetBucketIndex(const K &key) {
    size_t hash = HashKey(key);
    // use the last global_depth bits
    return static_cast<int>(hash & ((1 << global_depth) - 1));
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
