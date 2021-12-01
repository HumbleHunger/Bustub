/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * leaf,
                int index, BufferPoolManager *buffer_pool_manager)
								:	leaf_(leaf),
									index_(index),
									buffer_pool_manager_(buffer_pool_manager)
								{}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
	buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
	buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::IsEnd() {
	return (leaf_ == nullptr || (index_ >= leaf_->GetSize() && leaf_->GetNextPageId() == INVALID_PAGE_ID));
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
	if (IsEnd()) {
		throw std::out_of_range("IndexIterator: out of range");
	}
	return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
	++index_;
	if (index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
		page_id_t next_page_id = leaf_->GetNextPageId();

		auto *page = buffer_pool_manager_->FetchPage(next_page_id);
		if (page == nullptr) {
			throw Exception(ExceptionType::INDEX, "all page are pinned while IndexIterator(operator++)");
		}
		page->RLatch();

		buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
		buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);

		leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
		index_ = 0;
	}
	return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
