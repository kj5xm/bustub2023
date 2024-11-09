#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> cur = GetRoot();
  if (cur == nullptr) {
    return nullptr;
  }

  for (char ch : key) {
    if (cur->children_.count(ch) != 0) {
      cur = cur->children_.at(ch);
    } else {
      return nullptr;
    }
  }

  if (key.empty()) {
    cur = cur->children_.at('\0');
    if (cur == nullptr) {
      return nullptr;
    }
  }

  // auto node = dynamic_cast<const TrieNodeWithValue<T> *> (cur.get());
  auto node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur);
  if (node != nullptr) {
    return node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  auto root = this->GetRoot();
  auto cur = root;

  auto const new_root = root != nullptr ? std::shared_ptr<TrieNode>(root->Clone()) : std::make_shared<TrieNode>();
  // newCur is shared_ptr<TrieNode>, so we can modify children_.
  auto new_cur = new_root;

  if (key.empty()) {
    auto next_cur = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    new_root->children_['\0'] = next_cur;
    return Trie(new_root);
  }

  for (size_t i = 0; i < key.length(); i++) {
    char ch = key[i];
    // if the char is already in the trie, and it is not the last character in the key
    std::shared_ptr<TrieNodeWithValue<T>> next_cur = nullptr;
    if (i == key.length() - 1) {
      if (cur != nullptr && cur->children_.count(ch) != 0) {
        cur = cur->children_.at(ch);
        next_cur = std::make_shared<TrieNodeWithValue<T>>(cur->children_, std::make_shared<T>(std::move(value)));
        new_cur->children_[ch] = next_cur;
        new_cur = next_cur;
      } else {
        next_cur = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
        new_cur->children_[ch] = next_cur;
      }
      break;
    }

    if (cur != nullptr && cur->children_.count(ch) != 0) {
      // cur is shared_ptr<const TrieNode>
      cur = cur->children_.at(ch);
      // nextCur is shared_ptr<TrieNode>
      auto next_cur = cur->is_value_node_ ? std::shared_ptr<TrieNodeWithValue<T>>(
                                                std::static_pointer_cast<TrieNodeWithValue<T>, TrieNode>(cur->Clone()))
                                          : std::shared_ptr<TrieNode>(cur->Clone());

      new_cur->children_[ch] = next_cur;
      new_cur = next_cur;
    } else {
      auto next_cur = std::make_shared<TrieNode>();
      new_cur->children_[ch] = next_cur;
      new_cur = next_cur;
    }
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  auto const root = this->GetRoot();
  auto cur = root;

  // auto const newRoot = std::shared_ptr<TrieNode>(root->Clone());
  auto const new_root = root != nullptr ? std::shared_ptr<TrieNode>(root->Clone()) : std::make_shared<TrieNode>();

  if (key.empty()) {
    new_root->children_.erase('\0');
  }

  for (char ch : key) {
    if (cur->children_.count(ch) != 0) {
      cur = cur->children_.at(ch);
    } else {
      return Trie(new_root);
    }
  }

  if (!cur->is_value_node_) {
    return Trie(new_root);
  }

  std::vector<std::pair<char, std::shared_ptr<TrieNode>>> path;
  path.emplace_back('\0', new_root);

  cur = root;
  auto new_cur = new_root;

  // then create new trie by clone node in path
  for (char ch : key) {
    cur = cur->children_.at(ch);

    auto next_cur = cur->is_value_node_ ? std::shared_ptr<TrieNodeWithValue<class T>>(
                                              std::static_pointer_cast<TrieNodeWithValue<T>, TrieNode>(cur->Clone()))
                                        : std::shared_ptr<TrieNode>(cur->Clone());
    new_cur->children_[ch] = next_cur;
    new_cur = next_cur;
    path.emplace_back(ch, new_cur);
  }

  // reverse iterate the nodes in the path
  std::reverse(path.begin(), path.end());

  // the last node is the root, so no need to reach last node
  for (size_t i = 0; i < path.size() - 1; i++) {
    char ch = path[i].first;
    auto node = path[i].second;
    auto parent_node = path[i + 1].second;

    if (i == 0 && node->is_value_node_) {
      if (node->children_.empty()) {
        parent_node->children_.erase(ch);
      } else {
        std::shared_ptr<const TrieNode> new_node = std::make_shared<const TrieNode>(node->children_);
        parent_node->children_[ch] = new_node;
      }
    } else if (node->children_.empty() && !node->is_value_node_) {
      parent_node->children_.erase(ch);
    }
  }

  if (new_root->children_.empty()) {
    return {};
  }

  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
