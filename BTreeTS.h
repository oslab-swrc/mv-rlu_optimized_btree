// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cassert>
#include <cstring>
#include <atomic>
#include <immintrin.h>
#include <sched.h>
#include "../../../include/mvrlu.h"

namespace btreets {

enum class PageType : uint8_t { BTreeInner=1, BTreeLeaf=2 };

static const uint64_t pageSize = 128;

struct NodeBase {
  PageType type;
  uint16_t count;
};

struct BTreeLeafBase : public NodeBase {
   static const PageType typeMarker=PageType::BTreeLeaf;
};

template<class Key,class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   struct Entry {
      Key k;
      Payload p;
   };

   static const uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(Key)+sizeof(Payload));

   Key keys[maxEntries];
   Payload payloads[maxEntries];

   BTreeLeaf() {
      count=0;
      type=typeMarker;
   }

   bool isFull() { return count==maxEntries; };

   unsigned lowerBound(Key k) {
      unsigned lower=0;
      unsigned upper=count;
      do {
         unsigned mid=((upper-lower)/2)+lower;
         if (k<keys[mid]) {
            upper=mid;
         } else if (k>keys[mid]) {
            lower=mid+1;
         } else {
            return mid;
         }
      } while (lower<upper);
      return lower;
   }

   unsigned lowerBoundBF(Key k) {
      auto base=keys;
      unsigned n=count;
      while (n>1) {
         const unsigned half=n/2;
         base=(base[half]<k)?(base+half):base;
         n-=half;
      }
      return (*base<k)+base-keys;
   }

  void insert(Key k,Payload p) {
    assert(count<maxEntries);
    if (count) {
      unsigned pos=lowerBound(k);
      if ((pos<count) && (keys[pos]==k)) {
	// Upsert
	payloads[pos] = p;
	return;
      }
      memmove(keys+pos+1,keys+pos,sizeof(Key)*(count-pos));
      memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
      keys[pos]=k;
      payloads[pos]=p;
    } else {
      keys[0]=k;
      payloads[0]=p;
    }
    count++;
  }

   BTreeLeaf* split(Key& sep) {
      BTreeLeaf* newLeaf = (BTreeLeaf*) mvrlu_alloc(sizeof(BTreeLeaf));
      new (newLeaf) BTreeLeaf();
      newLeaf->count = count-(count/2);
      count = count-newLeaf->count;
      memcpy(newLeaf->keys, keys+count, sizeof(Key)*newLeaf->count);
      memcpy(newLeaf->payloads, payloads+count, sizeof(Payload)*newLeaf->count);
      sep = keys[count-1];
      return newLeaf;
   }
};

struct BTreeInnerBase : public NodeBase {
   static const PageType typeMarker=PageType::BTreeInner;
};

template<class Key>
struct BTreeInner : public BTreeInnerBase {
   static const uint64_t maxEntries=(pageSize-sizeof(NodeBase))/(sizeof(Key)+sizeof(NodeBase*));
   NodeBase* children[maxEntries];
   Key keys[maxEntries];

   BTreeInner() {
      count=0;
      type=typeMarker;
   }

   bool isFull() { return count==(maxEntries-1); };

   unsigned lowerBoundBF(Key k) {
      auto base=keys;
      unsigned n=count;
      while (n>1) {
         const unsigned half=n/2;
         base=(base[half]<k)?(base+half):base;
         n-=half;
      }
      return (*base<k)+base-keys;
   }

   unsigned lowerBound(Key k) {
      unsigned lower=0;
      unsigned upper=count;
      do {
         unsigned mid=((upper-lower)/2)+lower;
         if (k<keys[mid]) {
            upper=mid;
         } else if (k>keys[mid]) {
            lower=mid+1;
         } else {
            return mid;
         }
      } while (lower<upper);
      return lower;
   }

   BTreeInner* split(Key& sep) {
      BTreeInner<Key>* newInner= (BTreeInner*)mvrlu_alloc(sizeof(BTreeInner));
      new (newInner) BTreeInner<Key>();
      newInner->count=count-(count/2);
      count=count-newInner->count-1;
      sep=keys[count];
      memcpy(newInner->keys,keys+count+1,sizeof(Key)*(newInner->count+1));
      memcpy(newInner->children,children+count+1,sizeof(NodeBase*)*(newInner->count+1));
      return newInner;
   }

   void insert(Key k,NodeBase* child) {
      assert(count<maxEntries-1);
      unsigned pos=lowerBound(k);
      memmove(keys+pos+1,keys+pos,sizeof(Key)*(count-pos+1));
      memmove(children+pos+1,children+pos,sizeof(NodeBase*)*(count-pos+1));
      keys[pos]=k;
      mvrlu_assign_ptr(self, &children[pos], child);
      std::swap(children[pos],children[pos+1]);
      count++;
   }
};


template<class Key,class Value>
class BTree {
   private:
   BTreeInner<Key>* masterRoot;
   public:
   BTree() {
      masterRoot = (BTreeInner<Key>*)mvrlu_alloc(sizeof(BTreeInner<Key>));
      new (masterRoot) BTreeInner<Key>();
      masterRoot->children[0] = (NodeBase*)mvrlu_alloc(sizeof(BTreeLeaf<Key,Value>));
      new (masterRoot->children[0]) BTreeLeaf<Key, Value>();
      //printf("MasterRoot: %p\n",masterRoot);
   }

   int makeRoot(mvrlu_thread_struct_t *self, Key k, NodeBase* leftChild, NodeBase* rightChild) {
      BTreeInner<Key>* root = masterRoot;
      if (!mvrlu_try_lock(self, &root)){
              return 0;
      }
      //printf("MakeRoort\n");
      BTreeInner<Key>* inner = (BTreeInner<Key>*)mvrlu_alloc(sizeof(BTreeInner<Key>)); 
      new (inner) BTreeInner<Key>();
      inner->count = 1;
      inner->keys[0] = k;
      mvrlu_assign_ptr(self, &(inner->children[0]), leftChild);
      mvrlu_assign_ptr(self, &(inner->children[1]), rightChild);
      mvrlu_assign_ptr(self, &(root->children[0]), inner);
      return 1;
   }

  void yield(int count) {
    if (count>3)
      sched_yield();
    else
      _mm_pause();
  }

  void insert(mvrlu_thread_struct_t *self, Key k, Value v) {
    int restartCount = 0;
  restart:
    mvrlu_reader_lock(self);
    // Current node
    BTreeInner<Key>* root = (BTreeInner<Key>*)mvrlu_deref(self, masterRoot);
    NodeBase* node = (NodeBase*)mvrlu_deref(self, root->children[0]);
    // Parent of current node
    BTreeInner<Key>* parent = nullptr;

    while (node->type==PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key>*>(node);

      // Split eagerly if full
      if (inner->isFull()) {
	// Lock
	if (parent) {
	  if (!mvrlu_try_lock(self, &parent)) {
            mvrlu_abort(self);
            goto restart;
          }
	}
        if (!mvrlu_try_lock(self, &inner)) {
          mvrlu_abort(self);
          goto restart;
        }
	// Split
	Key sep; BTreeInner<Key>* newInner = inner->split(sep);
	if (parent)
	  parent->insert(sep,newInner);
	else {
	  if(!makeRoot(self, sep,inner,newInner)){
            mvrlu_abort(self);
            goto restart;
          }
        }
        mvrlu_reader_unlock(self);
        goto restart;
      }

      parent = inner;

      node = (NodeBase*)mvrlu_deref(self, inner->children[inner->lowerBound(k)]);
    }

    auto leaf = static_cast<BTreeLeaf<Key,Value>*>(node);

    // Split leaf if full
    if (leaf->count==leaf->maxEntries) {
      // Lock
      if (parent) {
        if (!mvrlu_try_lock(self, &parent)) {
          mvrlu_abort(self);
          goto restart;
        }
      }
      if (!mvrlu_try_lock(self, &leaf)) {
         mvrlu_abort(self);
         goto restart;
      }
      Key sep; BTreeLeaf<Key,Value>* newLeaf = leaf->split(sep);
      if (parent){
	parent->insert(sep, newLeaf);
      }
      else {
        if(!makeRoot(self, sep, leaf, newLeaf)){
           mvrlu_abort(self);
           goto restart;
        }
      }
      mvrlu_reader_unlock(self);
      goto restart;
    } else {
      if (!mvrlu_try_lock(self, &leaf)) {
        mvrlu_abort(self);
        goto restart;
      }
      leaf->insert(k, v);
    }
    //printf("unlock\n");
    mvrlu_reader_unlock(self);
    return; // success
  }

  bool lookup(mvrlu_thread_struct_t *self, Key k, Value& result) {
  restart:
    // Current node
    mvrlu_reader_lock(self);
    BTreeInner<Key>* root = (BTreeInner<Key>*)mvrlu_deref(self, masterRoot);
    NodeBase* node = (NodeBase*)mvrlu_deref(self, root->children[0]);

    // Parent of current node
    BTreeInner<Key>* parent = nullptr;

    while (node->type==PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key>*>(node);
      parent = inner;
      node = (NodeBase*)mvrlu_deref(self, inner->children[inner->lowerBound(k)]);
    }

    BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success;
    if ((pos<leaf->count) && (leaf->keys[pos]==k)) {
      success = true;
      result = leaf->payloads[pos];
    }
    mvrlu_reader_unlock(self);
    return success;
  }
/*
  uint64_t scan(Key k, int range, Value* output) {
    int restartCount = 0;
  restart:
    if (restartCount++)
      yield(restartCount);
    bool needRestart = false;

    NodeBase* node = root;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || (node!=root)) goto restart;

    // Parent of current node
    BTreeInner<Key>* parent = nullptr;
    uint64_t versionParent;

    while (node->type==PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key>*>(node);

      if (parent) {
	parent->readUnlockOrRestart(versionParent, needRestart);
	if (needRestart) goto restart;
      }

      parent = inner;
      versionParent = versionNode;

      node = inner->children[inner->lowerBound(k)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    }

    BTreeLeaf<Key,Value>* leaf = static_cast<BTreeLeaf<Key,Value>*>(node);
    unsigned pos = leaf->lowerBound(k);
    int count = 0;
    for (unsigned i=pos; i<leaf->count; i++) {
      if (count==range)
	break;
      output[count++] = leaf->payloads[i];
    }

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }
    node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;

    return count;
  }
*/

};

}
