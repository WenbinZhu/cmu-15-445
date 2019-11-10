/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
    std::lock_guard<std::mutex> guard(mutex);

    // if value already exists, first erase it from the list then re-insert
    auto pos = value_map.find(value);
    if (pos != value_map.end()) {
        access_list.erase(pos->second);
    }

    access_list.push_front(value);
    value_map.emplace(value, access_list.begin());
}

/*
 * If LRU is non-empty, pop the head member from LRU to argument "value",
 * and return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    std::lock_guard<std::mutex> guard(mutex);

    if (access_list.size() == 0) {
        return false;
    }

    value = access_list.back();
    access_list.pop_back();
    value_map.erase(value);

    return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
    std::lock_guard<std::mutex> guard(mutex);

    auto pos = value_map.find(value);
    if (pos == value_map.end()) {
        return false;
    }

    access_list.erase(pos->second);
    value_map.erase(value);

    return true;
}

template <typename T> size_t LRUReplacer<T>::Size() {
    std::lock_guard<std::mutex> guard(mutex);
    return access_list.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
