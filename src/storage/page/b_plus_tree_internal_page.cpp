//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  LOG_INFO("Create an internal page, id: %d", GetPageId());
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
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
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  /*
  for (int i = 1; i < GetSize(); ++i) {
    // 找到第一个比key大的
    if (comparator(key, array_[i].first) == -1) {
      // 返回key范围所属的page id
      return array_[i - 1].second;
    }
  }
  // 如果比所有储存的key都大，则返回最后一个value
  return array_[GetSize() - 1].second;
  */
  
  // binary search
  assert(GetSize() > 1);
  if (comparator(key, array_[1].first) < 0) {
    return array_[0].second;
  } else if (comparator(key, array_[GetSize() - 1].first) >= 0) {
    return array_[GetSize() - 1].second;
  }

  int low = 1, high = GetSize() - 1, mid;
  while (low < high && low + 1 != high) {
    mid = low + (high - low)/2;
    if (comparator(key, array_[mid].first) < 0) {
      high = mid;
    } else if (comparator(key, array_[mid].first) > 0) {
      low = mid;
    } else {
      return array_[mid].second;
    }
  }
  return array_[low].second;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // 必须是空节点 标记
  assert(GetSize() == 0);
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  IncreaseSize(2);// 标记
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 从后往前遍历，同时移动数组元素
  for (int i = GetSize(); i > 0; --i) {
    if (array_[i - 1].second == old_value) {
      array_[i] = {new_key, new_value};
      IncreaseSize(1);
      break;
    }
    array_[i] = array_[i - 1];
  }
  return GetSize();
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // 如果internal为空或者key比所有key都大
  if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array_[GetSize()] = {key, value};
    IncreaseSize(1);
    return GetSize();
  }

  // 从后往前遍历，同时移动数组元素
  for (int i = GetSize(); i > 0; --i) {
    // 找到第一个小于key的元素
    if (comparator(key, array_[i - 1].first) > 0) {
      array_[i] = {key, value};
      break;
    }
    array_[i] = array_[i - 1];
  }

  // 如果key比所有元素都小
  if (comparator(key, array_[0].first) < 0) {
    array_[0] = {key, value};
  }

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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                int mark,
                                                BufferPoolManager *buffer_pool_manager) {
  // 根据mark位决定哪边多一个
  int half = 0;
  if (mark == 0) half = (GetSize() + 1) / 2;
  else half = GetSize() / 2;
  recipient->CopyNFrom(array_ + GetSize() - half, half, buffer_pool_manager);

  for (auto index = GetSize() - half; index < GetSize(); ++index) {
    // 获得children page
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (!page) {
      throw Exception(ExceptionType::INDEX, "all page are pinned while CopyLastFrom");
    }
    // 获取child page并设置parent id
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
  IncreaseSize(-1 * half);                            
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // must be a new page
  assert(GetSize() + size <= GetMaxSize());
  int start = GetSize();
  for (int i = 0; i < size; ++i) {
    array_[start + i] = *items++;
  }
  // 标记
  IncreaseSize(size);
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
  assert(0 <= index && index < GetSize());
  // 将index后的kv pair往前覆盖一个偏移
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 将分隔key设置在0的位置
  SetKeyAt(0, middle_key);

  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);

  // 更新所有子page的父page id
  for (int index = 0; index < GetSize(); ++index) {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (page == nullptr) {
      throw Exception(ExceptionType::INDEX, "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // 将子page的parent设置为接收的page
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    // 通过buffer_pool做持久化处理
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
  // 必须在更新完child节点的parent id时执行
  IncreaseSize(-1 * GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 0);
  // 保存有用的kv对
  MappingType pair{middle_key, ValueAt(0)};
  // 获取需要更新的child节点
  page_id_t child_page_id = ValueAt(0);
  // 删除第一对kv
  Remove(0);

  recipient->CopyLastFrom(pair);

  // 更新child节点的parent id
  auto *page = buffer_pool_manager->FetchPage(child_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());

  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair) {
  assert(GetSize() + 1 <= GetMaxSize());
  // copy
  array_[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 0);
  // 删除最后一个节点
  IncreaseSize(-1);
  MappingType pair = array_[GetSize()];
  page_id_t child_page_id = pair.second;

  // delegate
  // 设置middle_key到recipient的虚拟节点
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(pair);

  // update child parent page id
  auto *page = buffer_pool_manager->FetchPage(child_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::INDEX, "all page are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());

  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);                  
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair) {
  assert(GetSize() + 1 < GetMaxSize());
  
  std::memmove((void *)(array_ + 1), (void *)(array_), GetSize()*sizeof(MappingType));
  array_[0] = pair;
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
