// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
#include <algorithm>

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  LockRequest lr(EXCLUSIVE, txn);

  // kalo gak ada di lock table
  if(this->lock_table_.find(key) == this->lock_table_.end()) {
    deque<LockRequest> *h = new deque<LockRequest>();
    h->push_back(lr);
    this->lock_table_[key] = h;
    return true;
  } else {

    // udah ada yg di queue, bisa granted kalau kosong
    this->lock_table_[key]->push_back(lr);
    if(this->lock_table_[key]->size() == 1) return true;

    if(this->txn_waits_.find(txn) == this->txn_waits_.end()) {
      this->txn_waits_[txn] = 1;
    } else {
      this->txn_waits_[txn]++;
    }

    return false;
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  //cek apakah front merupakan txn yang akan di release
  if(this->lock_table_[key]->front().txn_ == txn) {
    this->lock_table_[key]->pop_front();

    if(this->lock_table_[key]->size() > 0) {

      // append next transaction di key yang sama ke ready txn
      this->ready_txns_->push_back(this->lock_table_[key]->front().txn_);
    }
  } else {
    auto it = this->lock_table_[key]->begin();
    while(it->txn_ != txn) {
      it++;
    }

    this->lock_table_[key]->erase(it);
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!

  owners->clear();

  if(this->lock_table_[key]->size() > 0) {
      owners->push_back(this->lock_table_[key]->front().txn_);
  }

  if(owners->empty()) {
    return UNLOCKED;
  } else {
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

