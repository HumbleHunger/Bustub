//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <iostream>
#include <fstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

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
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 找到key所在的leaf page
  auto *node = FindLeafPage(key, false, Operation::READONLY, transaction);
  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node->GetData());
  bool ret = false;
  if (leaf != nullptr) {
    ValueType value;
    // 在leaf page中找对应的kv
    if (leaf->Lookup(key, value, comparator_)) {
      result->push_back(value);
      ret = true;
    }
    UnlockUnpinPages(Operation::READONLY, transaction);
    // 如果事务为nullptr，则需要对leaf page进行unlock和unpin
    if (transaction == nullptr) {
      auto page_id = leaf->GetPageId();

      buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
    }
  }
  return ret;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //std::cout << "Insert key : " << key << " value : " << value << std::endl;
  {
    // 如果b+ tree是空的则新建root节点
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
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
  //std::cout << "start new tree" << std::endl;
  auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while StartNewTree");
  }

  auto *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  // 更新root page后需要做的一些操作
  UpdateRootPageId(true);
  // 初始化new page
  root->Init(root_page_id_, INVALID_PAGE_ID);
  root->Insert(key, value, comparator_);

  // unpin root
  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
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
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 沿途加锁找到对应的leaf节点
  //std::cout << "Insert into leaf key : " << key << " value : " << value << std::endl;
  auto *node = FindLeafPage(key, false, Operation::INSERT, transaction);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node->GetData());
  if (leaf == nullptr) {
    return false;
  }

  // 在leaf节点中已经存在此key则返回false
  ValueType v;
  if (leaf->Lookup(key, v, comparator_)) {
    //std::cerr << "thread: " << transaction->GetThreadId() << ", key: " << key
    //          << " already exists" << std::endl;
    UnlockUnpinPages(Operation::INSERT, transaction);
    return false;
  }

  // 如果leaf节点中有空间则直接插入
  if (leaf->GetSize() < leaf_max_size_) {
    leaf->Insert(key, value, comparator_);
  } else {
    // 可能导致一个节点比另一个多两个
    // 如果没有空间加入，将leaf分裂成两个node, 再将新的kv插入到其中一个节点
    int mark = 0;
    if (comparator_(key, leaf->KeyAt(leaf_max_size_ / 2)) > 0) {
      mark = 1;
    }
    auto *leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf, mark);
    if (mark == 0) {
      leaf->Insert(key, value, comparator_);
    } else {
      leaf2->Insert(key, value, comparator_);
    }

    // 设置leaf page的next指针
    leaf2->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(leaf2->GetPageId());

    // 向父节点插入新leaf节点的信息
    InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
  }

  UnlockUnpinPages(Operation::INSERT, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, int mark) {
  //std::cout << "Split " << node->GetPageId() << std::endl;
  page_id_t page_id;
  // 分配new page
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while Split");
  }

  auto *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(page_id);

  // 将原page的一半kv对移植到新page
  node->MoveHalfTo(new_node, mark, buffer_pool_manager_);
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
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // 如果分裂的节点是root page
  if (old_node->IsRootPage()) {
    // 创建一个新的page作为root page
    auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::INDEX,
                      "all page are pinned while InsertIntoParent");
    }
    assert(page->GetPinCount() == 1);
    auto root =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());
    root->Init(root_page_id_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // update to new 'root_page_id'
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    // parent is done
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);

  } else {
    // 获取父page
    auto *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (page == nullptr) {
      throw Exception(ExceptionType::INDEX, "all page are pinned while InsertIntoParent");
    }
    auto *internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(page->GetData());

    // 如果内节点有空间则直接插入新的kv
    if (internal->GetSize() < internal_max_size_) {
      internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      // 设置new node 的parent
      new_node->SetParentPageId(internal->GetPageId());

      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    } else {
      // 如果parent node没有空间则需要分裂
      int mark = 0;
      if (comparator_(key, internal->KeyAt(internal_max_size_ / 2)) > 0) {
        mark = 1;
      }

      auto internal2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(internal, mark);

      if (mark) {
        internal2->Insert(key, new_node->GetPageId(), comparator_);
      } else {
        internal->Insert(key, new_node->GetPageId(), comparator_);
      }

      // 设置两个leaf节点的父节点
      // set new node's parent page id
      if (comparator_(key, internal2->KeyAt(0)) < 0) {
        new_node->SetParentPageId(internal->GetPageId());
      } else if (comparator_(key, internal2->KeyAt(0)) == 0) {
        new_node->SetParentPageId(internal2->GetPageId());
      } else {
        new_node->SetParentPageId(internal2->GetPageId());
        old_node->SetParentPageId(internal2->GetPageId());
      }

      // new_node is done, unpin it
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

      // 往上层递归
      InsertIntoParent(internal, internal2->KeyAt(0), internal2);
    }
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
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

  // 找到要删除的key所在的leaf page
  auto *node = FindLeafPage(key, false, Operation::DELETE, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(node->GetData());

  if (leaf != nullptr) {
    int size_before_deletion = leaf->GetSize();
    // 在leaf page中删除key
    if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion) {
      if (CoalesceOrRedistribute(leaf, transaction)) {
        transaction->AddIntoDeletedPageSet(leaf->GetPageId());
      }
    }
    UnlockUnpinPages(Operation::DELETE, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 返回为true说明node节点被删除
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  int max_size = 0;
  // 如果不需要合并则直接返回false
  if (node->IsLeafPage()) {
    max_size = leaf_max_size_;
    // 为什么leaf是不小于，而internal是大于?
    if (node->GetSize() >= (leaf_max_size_ + 1) / 2) {
      return false;
    }
  } else {
    max_size = internal_max_size_;
    if (node->GetSize() > internal_max_size_ / 2) {
      return false;
    }
  }
  // 如果需要合并或者重分配操作
  // 先获取parent page
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while CoalesceOrRedistribute");
  }
  auto parent = reinterpret_cast<InternalPage *>(page->GetData());
  
  // 寻找兄弟节点，如果可能尽量找到前一个(前驱节点)
    // 获得node在parent的index
  int value_index = parent->ValueIndex(node->GetPageId());
  int sibling_page_id = INVALID_PAGE_ID;
  if (value_index == 0) {
    // sibling是node的后驱
    sibling_page_id = parent->ValueAt(value_index + 1);
  } else {
    // sibling是node的前驱
    sibling_page_id = parent->ValueAt(value_index - 1);
  }

  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while CoalesceOrRedistribute");
  }

  // 对兄弟节点加写锁
  page->WLatch();
  transaction->AddIntoPageSet(page);
  auto sibling = reinterpret_cast<N *>(page->GetData());
  bool redistribute = false;

  // 根据两个node的size之和来判断是需要重新分配还是合并
  if (sibling->GetSize() + node->GetSize() > max_size) {
    redistribute = true;
    // release parent
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }

  // 执行重分配策略
  if (redistribute) {
    // 分情况
    if (value_index == 0) {
      Redistribute<N>(sibling, node, 0);
    } else {
      Redistribute<N>(sibling, node, 1);
    }
    return false;
  }

  // 执行合并策略
  bool ret = false;
  if (value_index == 0) {
    // 删除后节点sibling而不是node
    Coalesce<N>(node, sibling, parent, 1, transaction);
    // 将删除siblingg节点的记录保存起来
    transaction->AddIntoDeletedPageSet(sibling_page_id);
    ret = false;
  } else {
    // 删除后节点node
    Coalesce<N>(sibling, node, parent, value_index, transaction);
    ret = true;
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return ret;
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
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  // neighbbor_node是node的前节点，node节点是需要被删除的
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);

  // 删除node在parent中的kv信息
  parent->Remove(index);

  // 因为parent中删除了kv对，所以递归调用CoalesceOrRedistribute函数判断parent节点是否需要调整
  if (CoalesceOrRedistribute(parent, transaction)) {
    transaction->AddIntoDeletedPageSet(parent->GetPageId());
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
  // 获取父节点
  // 重分配总共有四种情况
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while MoveFirstToEndOf");
  }
  auto parent = reinterpret_cast<InternalPage *>(page->GetData());
  
  // 如果neighbor是node的后驱 （node -> neighbor）
  if (index == 0) {
    // leaf node
    if (node->IsLeafPage()) {
      neighbor_node->MoveFirstToEndOf(node);
      // 更新parent节点的相关kv对
      KeyType key = neighbor_node->KeyAt(0);
      parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), key);
    } else {
      // internal node
      // 从parent节点获得分隔key
      KeyType key;
      int index = parent->ValueIndex(neighbor_node->GetPageId());
      key = parent->KeyAt(index);
      neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
      // 更新parent中与neighbor相关的kv对
      parent->SetKeyAt(index, neighbor_node->KeyAt(0));
    }
  } else {
    // 如果neighbor是node的前驱 （neighbor -> node）
    if (node->IsLeafPage()) {
      neighbor_node->MoveLastToFrontOf(node);
      // 更新parent节点的相关kv对
      KeyType key = node->KeyAt(0);
      parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), key);
    } else {
      // 从parent节点获得分隔key
      KeyType key;
      int index = parent->ValueIndex(node->GetPageId());
      key = parent->KeyAt(index);
      neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
      // 更新parent中与node相关的kv对
      parent->SetKeyAt(index, node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
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
  // 如果root节点是leaf节点
  if (old_root_node->IsLeafPage()) {
    // 如果root节点的size为0了，则需要删除root
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }

  // 如果root节点是internal节点，且size等于1，则需要调整root page id
  if (old_root_node->GetSize() == 1) {
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    // 将root节点更改为old root的子节点
    root_page_id_ = root->ValueAt(0);
    // 调整hander_page
    UpdateRootPageId(false);

    // 更改新root的parent id为-1
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::INDEX, "all page are pinned while AdjustRoot");
    }
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto *page = FindLeafPage(key, false);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  int index = 0;
  if (leaf != nullptr) {
    index = leaf->KeyIndex(key, comparator_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf->GetNextPageId();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    page->RLatch();
    leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, leaf->GetSize(), buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Operation op, Transaction *transaction) {
  if (transaction == nullptr) return;

  // unlock以及unpin所有经过的parent page
  for (auto *page:*transaction->GetPageSet()) {
    if (op == Operation::READONLY) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();

  // 处理需要删除的page
  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();

  //std::cout << "unlockroot" << std::endl;
  // 如果root的mutex被此线程锁住则解锁
  if (root_is_locked_) {
    root_is_locked_ = false;
    unlockRoot();
  }

}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::isSafe(N *node, Operation op) {
  int max_size = 0;
  if (node->IsLeafPage()) max_size = leaf_max_size_;
  else max_size = internal_max_size_;
  int min_size = max_size / 2;

  if (op == Operation::INSERT) {
    return node->GetSize() < max_size;
  } else if (op == Operation::DELETE) {
    // >=: keep same with `coalesce logic`
    return node->GetSize() > min_size + 1;
  }
  return true;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction) {
  // 插入和删除操作在访问root page时需要将成员变量root_page_id_锁住
  if (op != Operation::READONLY) {
    lockRoot();
    root_is_locked_ = true;
  }

  // 如果树为空
  if (IsEmpty()) {
    return nullptr;
  }
  // 从根节点出发
  auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);
  if (parent == nullptr) {
    throw Exception(ExceptionType::INDEX,
                    "all page are pinned while FindLeafPage");
  }
  // 锁住root page
  if (op == Operation::READONLY) {
    parent->RLatch();
  } else {
    parent->WLatch();
  }
  // 在事务中保存root page
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(parent);
  }
  // 将page转为B+ tree中的page进行搜索，直到找到leaf node
  auto *node = reinterpret_cast<BPlusTreePage *>(parent->GetData());
  Page *child = parent;
  while (!node->IsLeafPage()) {
    auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(node);
    page_id_t child_page_id, parent_page_id = node->GetPageId();
    // 找到key所在的下一路径(page)
    if (leftMost) {
      child_page_id = internal->ValueAt(0);
    } else {
      child_page_id = internal->Lookup(key, comparator_);
    }

    child = buffer_pool_manager_->FetchPage(child_page_id);
    if (child == nullptr) {
      throw Exception(ExceptionType::INDEX, "all page are pinned while FindLeafPage");
    }

    if (op == Operation::READONLY) {
      // 获得child page的S锁
      child->RLatch();
      // 释放parent page的S锁
      UnlockUnpinPages(op, transaction);
    } else {
      // 写操作先只请求锁
      child->WLatch();
    }
    // 获得B+ tree page，改变node向下移动
    node = reinterpret_cast<BPlusTreePage *>(child->GetData());
    assert(node->GetParentPageId() == parent_page_id);
    // 插入和删除操作判断child page是否安全
    if (op != Operation::READONLY && isSafe(node, op)) {
      // 如果child是safe节点则释放之前在parent上获得的X锁
      UnlockUnpinPages(op, transaction);
    }

    if (transaction != nullptr) {
      transaction->AddIntoPageSet(child);
    } else {
      parent->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      parent = child;
    }
  }
  // 返回找到的leaf page
  return child;
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
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
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
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf_max_size_ << ",min_size=" << leaf_max_size_ / 2 << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << internal_max_size_ << ",min_size=" << internal_max_size_ / 2 << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
