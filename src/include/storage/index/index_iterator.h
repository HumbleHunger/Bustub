//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *,
                int, BufferPoolManager *);

  ~IndexIterator();

  bool IsEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  inline BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *GetLeaf() const { return leaf_; }
  inline int GetIndex() const { return index_; }
  inline BufferPoolManager *GetBuffer() const { return buffer_pool_manager_; }

  bool operator==(const IndexIterator &itr) const {
    if (itr.GetLeaf() == leaf_ && itr.GetIndex() == index_ && itr.GetBuffer() == buffer_pool_manager_) return true;
    else return false;
  }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

 private:
  // add your own private member variables here
  // 迭代器内部的原始指针(指向leaf节点)
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
  int index_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
