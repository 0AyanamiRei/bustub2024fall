#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

using std::cout;
using std::endl;

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::getkey(const KeyType &key) -> int64_t {
  int64_t kval;
  memcpy(&kval, key.data_, sizeof(int64_t));
  return kval;
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto tree_page = guard.AsMut<BPlusTreeHeaderPage>();
  tree_page->root_page_id_ = INVALID_PAGE_ID;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto tree_page = guard.As<BPlusTreeHeaderPage>();
  return tree_page->root_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  // 树为空
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  // 找到叶子节点
  page_id_t cursor_page_id = header_page->root_page_id_;
  ReadPageGuard cursor_guard = bpm_->ReadPage(cursor_page_id);
  const BPlusTreePage *cursor_page = cursor_guard.As<BPlusTreePage>();
  while(!cursor_page->IsLeafPage()) {
    const InternalPage *cursor_as_inner = cursor_guard.As<InternalPage>();
    cursor_page_id = cursor_as_inner->GetValByKey(key, comparator_);
    cursor_guard = bpm_->ReadPage(cursor_page_id);
    cursor_page = cursor_guard.As<BPlusTreePage>();
  }
  BUSTUB_ASSERT(cursor_page->IsLeafPage(), "错误的节点");
  // 叶子节点中寻找RID
  const LeafPage *cursor_as_inner = cursor_guard.As<LeafPage>();
  std::optional<bustub::RID> rid_opt = cursor_as_inner->GetValByKey(key, comparator_);
  if(rid_opt.has_value()) {
    result->push_back(rid_opt.value());
    return true;
  }
  return false;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  Context ctx;
  (void)ctx; /**< 用于消除未使用变量的警告 */
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto tree_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  // case1 树为空
  if (tree_page->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t root_page_id = bpm_->NewPage();  // 创建新的root
    auto root_guard = bpm_->WritePage(root_page_id);
    LeafPage *root_page = root_guard.AsMut<LeafPage>();
    root_page->Init(this->leaf_max_size_);
    root_page->push_back({key, value}); // 插入(key,val) pair
    tree_page->root_page_id_ = root_page_id; // 修改HeaderPage中的信息
    root_guard.Drop(); // 保证一下顺序, 先放root_page, 再放header_page
    return true;
  }
  ctx.root_page_id_ = tree_page->root_page_id_;
  GetLeafWritePageGuard(std::ref(key), std::ref(ctx), BtreeAccessType::Insert);
  WritePageGuard update_guard = std::move(ctx.write_set_.back());
  LeafPage *update_page = update_guard.AsMut<LeafPage>();
  ctx.write_set_.pop_back();
  // case2 node不满, 直接插入
  if (update_page->GetSize() < update_page->GetMaxSize()) {
    bool res = update_page->Insert2Leaf(key, value, comparator_); // 插入(key,val)
    return res;
  }

  // case3 node满, 需要分裂
  WritePageGuard right_guard = bpm_->WritePage(bpm_->NewPage());
  auto newkey_opt = update_page->SplitLeaf(std::ref(right_guard), {key, value}, this->comparator_);
  if (newkey_opt == std::nullopt) {  // 有重复key
    return false;
  }
  LeafPage *right_page = right_guard.AsMut<LeafPage>();
  (void)right_page; // for debug
  // update_guard -> left_guard + right_guard
  RecInsert2Inner(std::ref(ctx), std::move(update_guard), std::move(right_guard), newkey_opt.value());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto tree_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  /** case1 树为空 */
  if (tree_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // 从叶子节点中删除`key`
  ctx.root_page_id_ = tree_page->root_page_id_;
  GetLeafWritePageGuard(key, ctx, BtreeAccessType::Delete);
  WritePageGuard update_guard = std::move(ctx.write_set_.back());  // leaf node PageGuard
  LeafPage *update_page = update_guard.AsMut<LeafPage>();          // leaf node句柄
  ctx.write_set_.pop_back();

  if (!update_page->RemoveKey(key, comparator_)) {
    return;
  }
  /** case2 删除后节点仍保持不变量 */
  if (update_page->SizeInvariantCheck(0)) {
    return;
  }
  /** case3 删除后处于underflow condition */
  FixUnderflowLeaf(std::ref(ctx), update_page, std::move(update_guard), std::ref(key));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  page_id_t cursor_pid = GetRootPageId();
  ReadPageGuard cursor_guard = bpm_->ReadPage(cursor_pid);
  auto cursor_page = cursor_guard.As<BPlusTreePage>();

  while (!cursor_page->IsLeafPage()) {
    const InternalPage *cursor_as_inner = cursor_guard.As<InternalPage>();
    cursor_pid = cursor_as_inner->GetValByIndex(0);
    cursor_guard = bpm_->ReadPage(cursor_pid);
    cursor_page = cursor_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(std::move(cursor_guard), 0, bpm_);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard leaf_guard = GetLeafReadPageGuard(key);
  const LeafPage *leaf_page = leaf_guard.As<LeafPage>();
  std::optional<int> idx = leaf_page->GetIdxByKey(key, comparator_);
  return INDEXITERATOR_TYPE(std::move(leaf_guard), idx.value(), bpm_);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  page_id_t cursor_pid = GetRootPageId();
  ReadPageGuard cursor_guard = bpm_->ReadPage(cursor_pid);
  auto cursor_page = cursor_guard.As<BPlusTreePage>();

  while (!cursor_page->IsLeafPage()) {
    const InternalPage *cursor_as_inner = cursor_guard.As<InternalPage>();
    cursor_pid = cursor_as_inner->GetKVbyIndex(-1).second;
    cursor_guard = bpm_->ReadPage(cursor_pid);
    cursor_page = cursor_guard.As<BPlusTreePage>();
  }
  return INDEXITERATOR_TYPE(std::move(cursor_guard), cursor_page->GetSize() + 1, bpm_);
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto tree_page = guard.As<BPlusTreeHeaderPage>();
  return tree_page->root_page_id_;
}

// `ctx`记录从根节点到叶子节点的路径
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetLeafWritePageGuard(const KeyType &key, Context &ctx, BtreeAccessType access_type) {
  page_id_t cursor_pid = ctx.root_page_id_;  // root_page_id
  WritePageGuard cursor_guard = bpm_->WritePage(cursor_pid);
  BPlusTreePage *cursor_page = cursor_guard.AsMut<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(cursor_guard));  // 根节点压入队列
  if (cursor_page->IsLeafPage()) {
    return;
  }
  InternalPage *cursor_as_inner = ctx.write_set_.back().AsMut<InternalPage>();

  for (;;) {
    cursor_pid = cursor_as_inner->GetValByKey(key, this->comparator_);  // 找到所属区间page_id
    cursor_guard = bpm_->WritePage(cursor_pid);                       // 拿到下一层节点PageGuard
    cursor_page = cursor_guard.AsMut<BPlusTreePage>();
    /**< Latch crabbing-Safe */
    if (cursor_page->IsSafe(access_type)) {
      ctx.header_page_ = std::nullopt;
      while (!ctx.write_set_.empty()) {
        ctx.write_set_.pop_front();
      }
    }
    ctx.write_set_.emplace_back(std::move(cursor_guard));  // 移动
    if (cursor_page->IsLeafPage()) {
      return;
    }
    cursor_as_inner = ctx.write_set_.back().AsMut<InternalPage>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafReadPageGuard(const KeyType &key) -> ReadPageGuard {
  page_id_t cursor_pid = GetRootPageId();
  ReadPageGuard cursor_guard = bpm_->ReadPage(cursor_pid);
  auto cursor_page = cursor_guard.As<BPlusTreePage>();

  while (!cursor_page->IsLeafPage()) {
    const InternalPage *cursor_as_inner = cursor_guard.As<InternalPage>();
    // 找到所属区间
    cursor_pid = cursor_as_inner->GetValByKey(key, this->comparator_);
    cursor_guard = bpm_->ReadPage(cursor_pid);
    cursor_page = cursor_guard.As<BPlusTreePage>();
  }

  return cursor_guard;
}



// 下一层节点(不需要区分leaf和inner)发生分裂, 修改其父节点`cursor`
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RecInsert2Inner(Context &ctx, WritePageGuard &&left_guard, WritePageGuard &&right_guard,
                                     KeyType &key) {
  //  case1(递归终点: 创建新root)
  if (ctx.write_set_.empty()) {
    /** 创建新的root_page */
    page_id_t root_page_id = bpm_->NewPage();
    WritePageGuard root_guard = bpm_->WritePage(root_page_id);
    InternalPage *root_page = root_guard.AsMut<InternalPage>();
    root_page->Init(this->internal_max_size_);
    root_page->SetValByIndex(left_guard.GetPageId(), 0);
    root_page->SetKVbyIndex({key, right_guard.GetPageId()}, 1);
    root_page->IncreaseSize(1);
    root_guard.Drop();
    /** 修改header_page */
    BPlusTreeHeaderPage *tree_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    tree_page->root_page_id_ = root_page_id;
    return;
  }
  // 从ctx中弹出需要修改的父节点
  WritePageGuard update_guard = std::move(ctx.write_set_.back());
  InternalPage *update_page = update_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  // case2(递归终点) node不满, 直接插入
  if (!update_page->IsFull()) {
    update_page->Insert2Inner(key, right_guard.GetPageId(), comparator_);
    return;
  }
  // case3(往上递归) node满, 需要分裂
  WritePageGuard new_right_guard = bpm_->WritePage(bpm_->NewPage());
  // 两个内部节点left_guard(旧节点), new_right_guard 以及往上层递归插入的newkey
  auto newkey_opt = update_page->SplitInner(new_right_guard, {key, right_guard.GetPageId()}, this->comparator_);
  // update_guard -> new_left_guard + new_right_guard
  RecInsert2Inner(std::ref(ctx), std::move(update_guard), std::move(new_right_guard), newkey_opt.value());
}


// 恢复underflow condition下的叶子节点
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FixUnderflowLeaf(Context &ctx, LeafPage *leaf_page, WritePageGuard &&leafpage_guard,
                                      const KeyType &key) {
  // leaf_page就是root_page
  if (ctx.write_set_.empty()) {
    if (leaf_page->GetSize() == 0) { /** 删除root_page */
      auto head_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
      head_page->root_page_id_ = INVALID_PAGE_ID;
      LOG_INFO("修改为空树");
    }
    return;
  }
  WritePageGuard parent_guard, left_guard, right_guard;
  InternalPage *parent_page(nullptr);
  LeafPage *left_page(nullptr), *right_page(nullptr);
  page_id_t right_page_id, left_page_id;
  // 获取父节点
  parent_guard = std::move(ctx.write_set_.back());
  parent_page = parent_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  /** */
  int idx2update = parent_page->GetIndexByKey(key, comparator_);
  // 只有当idx2update > 0 才存在左兄弟
  left_page_id = (idx2update > 0) ? parent_page->GetValByIndex(idx2update - 1) : INVALID_PAGE_ID ;
  // 只有当idx2update < parent_page->GetSize() 才有右兄弟
  right_page_id = (idx2update < parent_page->GetSize()) ? parent_page->GetValByIndex(idx2update + 1) : INVALID_PAGE_ID;
  if(left_page_id == leafpage_guard.GetPageId()) {
    LOG_INFO("父节点中查询%ld, 返回idx=%d, 父节点size=%d", getkey(key), idx2update, parent_page->GetSize());
    LOG_INFO("left_page_id=%d, leafpage_page_id=%d",left_page_id, leafpage_guard.GetPageId());
    parent_page->Debug();
  }
  BUSTUB_ASSERT(left_page_id != leafpage_guard.GetPageId(), "左节点page_id错误");
  BUSTUB_ASSERT(right_page_id != leafpage_guard.GetPageId(), "右节点page_id错误");
  /** */
  if (left_page_id != INVALID_PAGE_ID) {  // 尝试获取左节点
    left_guard = bpm_->WritePage(left_page_id);
    left_page = left_guard.AsMut<LeafPage>();
  }
  if (right_page_id != INVALID_PAGE_ID) {  // 尝试获取右节点
    right_guard = bpm_->WritePage(right_page_id);
    right_page = right_guard.AsMut<LeafPage>();
  }
  
  /** */
  if (left_page_id != INVALID_PAGE_ID && left_page->SizeInvariantCheck(-1)) {  // push_front左节点末尾kv, 并更新父节点key
    leaf_page->push_front(left_page->pop_back());
    parent_page->SetKeyAt(idx2update, leaf_page->GetKeyByIndex(0));
  } else if (right_page_id != INVALID_PAGE_ID && right_page->SizeInvariantCheck(-1)) {  // push_back右节点头部kv, 并更新父节点kv
    leaf_page->push_back(right_page->pop_front());
    parent_page->SetKeyAt(idx2update + 1, right_page->GetKeyByIndex(0));
  } else if (left_page_id != INVALID_PAGE_ID) {  // 将自身合并到左节点
    KeyType rval = leaf_page->GetKeyByIndex(0);
    int i_idx = parent_page->GetIndexByKey(rval, comparator_);  // 即找到在父节点中要删除kv的下标
    BUSTUB_ASSERT(parent_page->GetValByIndex(i_idx) == leafpage_guard.GetPageId(), "父节点中的指针不对");
    left_page->MergeA2this(std::move(leafpage_guard));
    RemoveInternalPage(std::ref(ctx), parent_page, std::move(parent_guard), i_idx);
  } else if (right_page_id != INVALID_PAGE_ID) {  // 吞并右节点
    KeyType rval = right_page->GetKeyByIndex(0);
    int i_idx = parent_page->GetIndexByKey(rval, comparator_);  // 即找到在父节点中要删除kv的下标
    BUSTUB_ASSERT(parent_page->GetValByIndex(i_idx) == right_guard.GetPageId(), "父节点中的指针不对");
    leaf_page->MergeA2this(std::move(right_guard));                        // 吞并右节点
    RemoveInternalPage(std::ref(ctx), parent_page, std::move(parent_guard), i_idx);
  } else {  // 仅剩自己了
    // 我在RemoveInternalPage()处理了删到只剩下1个root和1个leaf的情况
    // 即: 删完最后第二个leaf后就销毁root, 将剩下的leaf变成root
    LOG_DEBUG("5");
  }
}

// 递归删除父节点中的array_[idx]
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternalPage(Context &ctx, InternalPage *update_page, WritePageGuard &&update_pageguard,
                                        int idx) {
  std::pair<KeyType, page_id_t> kv = update_page->GetKVbyIndex(idx);
  update_page->DeleteKVByIdx(idx);
  // case1(递归终点) 根节点
  if (ctx.write_set_.empty()) {
    if (update_page->GetSize() == 0U) {  // 只剩下1个leaf和1个root, 释放root, 修改leaf为新的root
      BPlusTreeHeaderPage *tree_page = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
      tree_page->root_page_id_ = update_page->GetValByIndex(0);
    }
    return;
  }
  // case2(递归终点) 删除后仍保持不变量
  if (update_page->SizeInvariantCheck(0)) {
    return;
  }
  // case3(往上递归) 删除后处于underflow condition
  WritePageGuard parent_guard, left_guard, right_guard;
  InternalPage *parent_page(nullptr), *left_page(nullptr), *right_page(nullptr);
  page_id_t right_page_id, left_page_id;
  // 获取父节点
  parent_guard = std::move(ctx.write_set_.back());
  parent_page = parent_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  // B+树对内部节点来说, 如果当前这层还有父节点, 那么这层的节点数b>=2
  int idx2update = parent_page->GetIndexByKey(kv.first, comparator_);
  left_page_id = (idx2update > 0) ? parent_page->GetValByIndex(idx2update - 1) : INVALID_PAGE_ID;
  right_page_id = (idx2update < parent_page->GetSize()) ? parent_page->GetValByIndex(idx2update + 1) : INVALID_PAGE_ID;
  BUSTUB_ASSERT(left_page_id != update_pageguard.GetPageId(), "左节点page_id错误");
  BUSTUB_ASSERT(right_page_id != update_pageguard.GetPageId(), "右节点page_id错误");
  /** */
  if (left_page_id != INVALID_PAGE_ID) {  // 尝试获取左节点
    left_guard = bpm_->WritePage(left_page_id);
    left_page = left_guard.AsMut<InternalPage>();
  }
  if (right_page_id != INVALID_PAGE_ID) {  // 尝试获取右节点
    right_guard = bpm_->WritePage(right_page_id);
    right_page = right_guard.AsMut<InternalPage>();
  }
  /** */
  if (left_page_id != INVALID_PAGE_ID && left_page->SizeInvariantCheck(-1)) {  // 从左兄弟获取kv
    auto left_kv = left_page->pop_back();  // 弹出左兄弟队尾kv
    update_page->SetKeyAt(0, parent_page->GetKVbyIndex(idx2update).first); // 将父节点key拷贝到自己
    update_page->push_front(left_kv);  // 压入左兄弟队尾kv
    parent_page->SetKeyAt(idx2update, left_kv.first); // 设置父节点key为左兄弟队尾kv.first
  } else if (right_page_id != INVALID_PAGE_ID && right_page->SizeInvariantCheck(-1)) {  // 从右兄弟获取kv
    auto right_kv = right_page->pop_front();  // 弹出右兄弟队头kv
    update_page->Insert2InnerByIdx({parent_page->GetKVbyIndex(idx2update + 1).first, right_kv.second}, update_page->GetSize() + 1);
    parent_page->SetKeyAt(idx2update + 1, right_page->GetKVbyIndex(0).first);
  } else if (left_page_id != INVALID_PAGE_ID) {  // 合并到左兄弟
    auto *update_array = update_page->GetArray();
    auto *left_array = left_page->GetArray();
    int sz = left_page->GetSize();
    left_array[++sz] = {parent_page->GetKVbyIndex(idx2update).first, update_array[0].second};
    for (int i = 1, n = update_page->GetSize(); i <= n; i++) {
      left_array[++sz] = update_array[i];
    }
    left_page->SetSize(sz);
    RemoveInternalPage(ctx, parent_page, std::move(parent_guard), idx2update);
  } else if (right_page_id != INVALID_PAGE_ID) {  // 吞并右兄弟
    auto *rigth_array = right_page->GetArray();
    auto *update_array = update_page->GetArray();
    int sz = update_page->GetSize();
    update_array[++sz] = {parent_page->GetKVbyIndex(idx2update+1).first, rigth_array[0].second};
    for (int i = 1, n = right_page->GetSize(); i <= n; i++) {
      update_array[++sz] = rigth_array[i];
    }
    update_page->SetSize(sz);
    RemoveInternalPage(ctx, parent_page, std::move(parent_guard), idx2update + 1);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
