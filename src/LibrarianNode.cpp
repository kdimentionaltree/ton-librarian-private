/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "LibrarianNode.h"

#include <algorithm>
#include <utility>

#include "vm/cells.h"
#include "vm/vm.h"

namespace tonlibrarian {

namespace {
constexpr double kTipPollInterval = 2.0;
constexpr double kChaseRetryInterval = 2.0;
constexpr double kStatsInterval = 60.0;
constexpr double kMcIdRetryInterval = 1.0;
constexpr double kFetchRetryInterval = 1.0;
constexpr double kHeartbeatInterval = 5.0;
// after this many failed rounds (each round already retries 10x inside
// LiteserverSource) an account's state is skipped instead of stalling the
// whole pipeline forever
constexpr td::uint32 kStateFetchMaxAttempts = 5;

const char* mode_name(Config::SyncMode mode) {
  switch (mode) {
    case Config::SyncMode::Liteservers:
      return "liteservers";
    case Config::SyncMode::Overlay:
      return "overlay";
    case Config::SyncMode::Hybrid:
      return "hybrid";
    default:
      return "?";
  }
}

std::string account_str(const Account& a) {
  return (PSLICE() << static_cast<int>(a.wc) << ":" << a.addr.to_hex()).str();
}

}  // namespace

td::actor::ActorOwn<LibrarianNode> LibrarianNode::create(Config config, std::atomic<bool>* done) {
  return td::actor::create_actor<LibrarianNode>("librarian-node", std::move(config), done);
}

LibrarianNode::LibrarianNode(Config config, std::atomic<bool>* done) : config_(std::move(config)), done_(done) {
}

// ---------------------------------------------------------------------------
// Setup.
// ---------------------------------------------------------------------------

void LibrarianNode::start_up() {
  started_at_ = td::Timestamp::now();
  vm::init_vm().ensure();

  storage_ = std::make_unique<Storage>(config_.db_path);
  auto st = storage_->open();
  if (st.is_error()) {
    LOG(FATAL) << "[librarian] failed to open storage at " << config_.db_path << ": " << st;
  }
  auto progress = storage_->get_progress();

  if (config_.mode == Config::SyncMode::Liteservers || config_.mode == Config::SyncMode::Hybrid) {
    auto r = LiteserverSource::create(config_.liteservers.global_config_path);
    if (r.is_error()) {
      LOG(FATAL) << "[librarian] failed to create liteserver source: " << r.move_as_error();
    }
    liteserver_ = r.move_as_ok();
  }
  if (use_overlay_blocks()) {
    auto r = OverlaySource::create(config_.overlay);
    if (r.is_error()) {
      LOG(FATAL) << "[librarian] failed to create overlay source: " << r.move_as_error();
    }
    overlay_ = r.move_as_ok();
  }
  init_parse_workers();

  LOG(INFO) << "[librarian] starting: mode=" << mode_name(config_.mode) << " db=" << config_.db_path
            << " committed=" << progress.mc_seqno << " from_seqno=" << config_.from_seqno
            << " until_seqno=" << config_.until_seqno << (config_.account_states.enabled ? " states=on" : " states=off");

  if (progress.mc_seqno > 0) {
    // continue from the existing database
    next_commit_seqno_ = progress.mc_seqno + 1;
    start_seqno_known_ = true;
    ton::BlockIdExt last{ton::masterchainId, ton::shardIdAll, progress.mc_seqno, progress.mc_root_hash,
                         progress.mc_file_hash};
    if (config_.mode == Config::SyncMode::Overlay) {
      chase_last_ = last;
      chase_ready_ = true;
    }
    tip_known_ = true;
    tip_seqno_ = progress.mc_seqno;
    ensure_slices();
    if (chase_ready_) {
      pump_chase();
    }
  } else if (config_.from_seqno > 0) {
    next_commit_seqno_ = config_.from_seqno;
    start_seqno_known_ = true;
    ensure_slices();
  } else {
    // fresh database: start at the current tip
    start_at_tip_ = true;
    if (config_.mode == Config::SyncMode::Overlay) {
      auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockIdExt> R) {
        td::actor::send_closure(SelfId, &LibrarianNode::on_overlay_init_block, std::move(R));
      });
      td::actor::send_closure(overlay_, &OverlaySource::get_init_block, std::move(P));
    }
    // liteserver modes: the first tip poll (alarm) determines the start seqno
  }

  next_tip_poll_ = td::Timestamp::in(0.2);
  next_stats_ = td::Timestamp::in(kStatsInterval);
  alarm_timestamp() = td::Timestamp::now();
}

void LibrarianNode::init_parse_workers() {
  // workers live on scheduler nodes 1..N (node 0 is the coordinator);
  // see main.cpp for the scheduler layout.
  td::uint32 n = std::max<td::uint32>(1, config_.pipeline.parse_workers);
  for (td::uint32 i = 0; i < n; i++) {
    workers_.push_back(td::actor::create_actor<ParseWorker>(
        td::actor::ActorOptions().with_name("parse-worker").on_scheduler(
            td::actor::SchedulerId{static_cast<td::uint8>(1 + i)})));
  }
}

void LibrarianNode::on_overlay_init_block(td::Result<ton::BlockIdExt> init_block) {
  if (init_block.is_error()) {
    LOG(FATAL) << "[librarian] cannot determine init block from global config: " << init_block.move_as_error();
  }
  auto id = init_block.move_as_ok();
  LOG(INFO) << "[librarian] fresh db in overlay mode: starting at init block " << id.to_str();
  next_commit_seqno_ = id.id.seqno;
  start_seqno_known_ = true;
  start_at_tip_ = false;
  mc_ids_[id.id.seqno] = id;
  tip_known_ = true;
  tip_seqno_ = id.id.seqno;
  chase_last_ = id;
  chase_ready_ = true;
  ensure_slices();
  pump_chase();
}

void LibrarianNode::tear_down() {
  if (storage_) {
    auto p = storage_->get_progress();
    LOG(INFO) << "[librarian] stopping at mc_seqno " << p.mc_seqno;
  }
}

// ---------------------------------------------------------------------------
// Pipeline control.
// ---------------------------------------------------------------------------

void LibrarianNode::ensure_slices() {
  if (stopping_ || !start_seqno_known_) {
    return;
  }
  td::uint32 upper = next_commit_seqno_ + std::max<td::uint32>(1, config_.pipeline.depth);
  for (td::uint32 s = next_commit_seqno_; s < upper; s++) {
    if (config_.until_seqno != 0 && s > config_.until_seqno) {
      break;
    }
    if (tip_known_ && s > tip_seqno_) {
      break;
    }
    if (tasks_.count(s) != 0) {
      continue;
    }
    start_task(s);
  }
  maybe_finish();
}

void LibrarianNode::start_task(td::uint32 seqno) {
  auto& task = tasks_[seqno];
  task.seqno = seqno;
  resolve_mc_id(task);
}

void LibrarianNode::drive_task(SliceTask& task) {
  switch (task.phase) {
    case Phase::NeedMcId:
      resolve_mc_id(task);
      break;
    case Phase::FetchMc:
      fetch_mc(task);
      break;
    case Phase::PrevTops:
      start_prev_tops(task);
      break;
    case Phase::Walk:
      pump_walk(task);
      break;
    case Phase::States:
      pump_states(task);
      break;
    case Phase::ParseMc:
    case Phase::ParsePrevTops:
    case Phase::Ready:
      break;
  }
}

void LibrarianNode::resolve_mc_id(SliceTask& task) {
  if (task.phase != Phase::NeedMcId) {
    return;
  }
  auto it = mc_ids_.find(task.seqno);
  if (it != mc_ids_.end()) {
    task.mc_id = it->second;
    task.phase = Phase::FetchMc;
    fetch_mc(task);
    return;
  }
  if (!liteserver_.empty()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno = task.seqno](
                                             td::Result<ton::BlockIdExt> R) {
      td::actor::send_closure(SelfId, &LibrarianNode::on_lookup, seqno, std::move(R));
    });
    td::actor::send_closure(liteserver_, &LiteserverSource::lookup_mc_block, task.seqno, std::move(P));
  } else {
    // pure overlay: the chase will discover the id
    task.retry_at = td::Timestamp::in(kMcIdRetryInterval);
  }
}

void LibrarianNode::fetch_mc(SliceTask& task) {
  if (task.phase != Phase::FetchMc) {
    return;
  }
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), seqno = task.seqno](td::Result<td::BufferSlice> R) {
        td::actor::send_closure(SelfId, &LibrarianNode::on_mc_data, seqno, std::move(R));
      });
  fetch_block_data(task.mc_id, std::move(P));
}

void LibrarianNode::start_prev_tops(SliceTask& task) {
  if (task.phase != Phase::PrevTops || task.have_prev_tops) {
    return;
  }
  // 1. previous tops may already be in the database
  auto last = storage_->get_last_tops_seqno();
  if (last && *last + 1 >= task.seqno) {
    auto tops = storage_->load_prev_tops(task.seqno - 1);
    for (auto& t : tops) {
      task.prev_tops.insert(t.id);
    }
    task.have_prev_tops = true;
    start_walk(task);
    return;
  }
  // 2. bootstrap: fetch masterchain block S-1 (its prev refs are trusted data)
  if (task.mc.prev.empty()) {
    if (task.seqno <= 1) {
      // genesis-adjacent slice: no previous tops to subtract
      task.have_prev_tops = true;
      start_walk(task);
      return;
    }
    LOG(ERROR) << "[librarian] slice " << task.seqno << ": cannot bootstrap prev tops (mc block has no prev)";
    task.retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  task.prev_tops_id = task.mc.prev[0];
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), seqno = task.seqno](td::Result<td::BufferSlice> R) {
        td::actor::send_closure(SelfId, &LibrarianNode::on_prev_tops_data, seqno, std::move(R));
      });
  fetch_block_data(task.prev_tops_id, std::move(P));
}

void LibrarianNode::start_walk(SliceTask& task) {
  task.phase = Phase::Walk;
  task.visited.clear();
  task.walk_queue.clear();
  for (const auto& top : task.mc.tops) {
    CHECK(!top.is_masterchain());
    if (task.prev_tops.count(top.id) != 0) {
      continue;  // shard unchanged: no new blocks under this top
    }
    if (task.visited.insert(top.id).second) {
      task.walk_queue.push_back(top);
    }
  }
  task.walk_outstanding = static_cast<td::uint32>(task.walk_queue.size());
  if (task.walk_outstanding == 0) {
    start_states(task);
    return;
  }
  pump_walk(task);
}

void LibrarianNode::pump_walk(SliceTask& task) {
  if (task.phase != Phase::Walk) {
    return;
  }
  if (task.retry_at && !task.retry_at.is_in_past()) {
    return;
  }
  while (!task.walk_queue.empty() && block_fetches_in_flight_ < std::max<td::uint32>(1, config_.pipeline.block_fetch_parallelism)) {
    auto id = task.walk_queue.front();
    task.walk_queue.erase(task.walk_queue.begin());
    block_fetches_in_flight_++;
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), seqno = task.seqno, id](td::Result<td::BufferSlice> R) {
          td::actor::send_closure(SelfId, &LibrarianNode::on_walk_data, seqno, id, std::move(R));
        });
    fetch_block_data(id, std::move(P));
  }
}

void LibrarianNode::start_states(SliceTask& task) {
  task.phase = Phase::States;
  if (!config_.account_states.enabled) {
    finish_slice(task);
    return;
  }
  // liteserver availability for states is guaranteed by config validation
  std::set<Account> accounts;
  for (const auto& b : task.blocks) {
    for (const auto& tx : b.txs) {
      accounts.insert(tx.account);
    }
  }
  for (const auto& tx : task.mc.txs) {
    accounts.insert(tx.account);
  }
  for (const auto& acc : accounts) {
    StateTask st;
    st.account = acc;
    task.state_tasks.push_back(std::move(st));
  }
  task.states_pending = static_cast<td::uint32>(task.state_tasks.size());
  if (task.states_pending == 0) {
    finish_slice(task);
    return;
  }
  pump_states(task);
}

void LibrarianNode::pump_states(SliceTask& task) {
  if (task.phase != Phase::States) {
    return;
  }
  if (task.retry_at && !task.retry_at.is_in_past()) {
    return;
  }
  for (td::uint32 i = 0; i < task.state_tasks.size(); i++) {
    auto& st = task.state_tasks[i];
    if (st.done || st.fetching) {
      continue;
    }
    if (state_fetches_in_flight_ >= std::max<td::uint32>(1, config_.pipeline.state_fetch_parallelism)) {
      break;
    }
    st.fetching = true;
    state_fetches_in_flight_++;
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), seqno = task.seqno, i = i](td::Result<td::BufferSlice> R) {
          td::actor::send_closure(SelfId, &LibrarianNode::on_state_data, seqno, i, std::move(R));
        });
    td::actor::send_closure(liteserver_, &LiteserverSource::get_account_state, task.mc_id, st.account,
                            std::move(P));
  }
}

void LibrarianNode::finish_slice(SliceTask& task) {
  task.phase = Phase::Ready;
  try_commit();
}

void LibrarianNode::try_commit() {
  while (!stopping_) {
    auto it = tasks_.find(next_commit_seqno_);
    if (it == tasks_.end() || it->second.phase != Phase::Ready) {
      break;
    }
    SliceTask& task = it->second;

    SliceData slice;
    slice.mc_seqno = task.seqno;
    slice.mc_id = task.mc_id;
    slice.mc_utime = task.mc.gen_utime;
    slice.tops = std::move(task.mc.tops);
    slice.blocks.reserve(1 + task.blocks.size());
    slice.blocks.push_back(std::move(task.mc));
    for (auto& b : task.blocks) {
      slice.blocks.push_back(std::move(b));
    }
    slice.account_states = std::move(task.account_states);
    // last_trans_hash is not part of the Account cell itself; recover it from
    // the slice's transactions (the account's latest tx of this slice)
    {
      std::map<Account, const ParsedTransaction*> last_tx;
      for (const auto& b : slice.blocks) {
        for (const auto& tx : b.txs) {
          auto it = last_tx.find(tx.account);
          if (it == last_tx.end() || it->second->lt < tx.lt) {
            last_tx[tx.account] = &tx;
          }
        }
      }
      for (auto& [account, st] : slice.account_states) {
        auto it = last_tx.find(account);
        if (it != last_tx.end()) {
          st.last_trans_lt = it->second->lt;
          st.last_trans_hash = it->second->hash;
        }
      }
    }
    slice.states_indexed = config_.account_states.enabled && !task.states_gave_up;

    td::uint64 txs = 0, msgs = 0;
    for (auto& b : slice.blocks) {
      txs += b.txs.size();
      msgs += b.messages.size();
    }

    double t0 = td::Time::now();
    auto st = storage_->commit_slice(slice);
    if (st.is_error()) {
      // a partially committed slice must never happen (single WriteBatch);
      // failing hard keeps the database trustworthy
      LOG(FATAL) << "[librarian] failed to commit slice " << task.seqno << ": " << st;
    }
    LOG(INFO) << "[librarian] committed slice " << task.seqno << ": " << slice.blocks.size() << " blocks, " << txs
             << " txs, " << msgs << " msgs, " << slice.account_states.size() << " states in "
             << (td::Time::now() - t0) << "s";

    tasks_.erase(it);
    next_commit_seqno_++;
    while (!mc_ids_.empty() && mc_ids_.begin()->first < next_commit_seqno_) {
      mc_ids_.erase(mc_ids_.begin());
    }
  }
  maybe_finish();
  ensure_slices();
}

void LibrarianNode::maybe_finish() {
  if (stopping_ || config_.until_seqno == 0 || !start_seqno_known_) {
    return;
  }
  if (next_commit_seqno_ > config_.until_seqno) {
    finish((PSLICE() << "until_seqno " << config_.until_seqno << " reached").str());
  }
}

void LibrarianNode::finish(std::string reason) {
  if (stopping_) {
    return;
  }
  stopping_ = true;
  LOG(INFO) << "[librarian] " << reason << "; stopping";
  if (done_) {
    done_->store(true);
  }
  stop();
}

void LibrarianNode::pump_all_walks() {
  for (auto& [seqno, task] : tasks_) {
    if (task.phase == Phase::Walk) {
      pump_walk(task);
    }
  }
}

void LibrarianNode::pump_all_states() {
  for (auto& [seqno, task] : tasks_) {
    if (task.phase == Phase::States) {
      pump_states(task);
    }
  }
}

// ---------------------------------------------------------------------------
// Async callbacks.
// ---------------------------------------------------------------------------

void LibrarianNode::on_tip(td::Result<ton::BlockIdExt> tip) {
  if (stopping_ || tip.is_error()) {
    return;  // the next poll retries
  }
  auto id = tip.move_as_ok();
  mc_ids_[id.id.seqno] = id;
  tip_known_ = true;
  tip_seqno_ = std::max(tip_seqno_, id.id.seqno);
  if (start_at_tip_ && !start_seqno_known_) {
    start_seqno_known_ = true;
    next_commit_seqno_ = id.id.seqno;
    LOG(INFO) << "[librarian] fresh db: starting at current tip " << id.id.seqno;
  }
  ensure_slices();
}

void LibrarianNode::on_lookup(td::uint32 seqno, td::Result<ton::BlockIdExt> id) {
  if (stopping_) {
    return;
  }
  SliceTask* task = find_task(seqno);
  if (task == nullptr) {
    return;
  }
  if (id.is_error()) {
    // block does not exist yet (or liteserver trouble): retry later
    if (task->phase == Phase::NeedMcId) {
      task->retry_at = td::Timestamp::in(kMcIdRetryInterval);
    }
    return;
  }
  mc_ids_[seqno] = id.move_as_ok();
  if (task->phase == Phase::NeedMcId) {
    resolve_mc_id(*task);
  }
}

void LibrarianNode::on_mc_data(td::uint32 seqno, td::Result<td::BufferSlice> data) {
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::FetchMc) {
    return;
  }
  if (data.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to fetch mc block: " << data.move_as_error();
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  task->phase = Phase::ParseMc;
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<ParsedBlock> R) {
    td::actor::send_closure(SelfId, &LibrarianNode::on_mc_parsed, seqno, std::move(R));
  });
  dispatch_parse_block(task->mc_id, data.move_as_ok(), std::move(P));
}

void LibrarianNode::on_mc_parsed(td::uint32 seqno, td::Result<ParsedBlock> parsed) {
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::ParseMc) {
    return;
  }
  if (parsed.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to parse mc block: " << parsed.move_as_error();
    task->phase = Phase::FetchMc;  // refetch (bad data / bad peer)
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  task->mc = parsed.move_as_ok();
  if (task->mc.tops.empty()) {
    // A masterchain block may legitimately have no shardchains (e.g. early
    // chain, or a masterchain-only configuration): the slice then contains
    // just the mc block itself.
    LOG(INFO) << "[librarian] slice " << seqno << ": mc block parsed with no shard tops";
  }
  LOG(INFO) << "[librarian] slice " << seqno << ": mc block parsed, " << task->mc.tops.size() << " shard tops, "
            << task->mc.txs.size() << " mc txs";

  // keep the overlay subscriptions in sync with the shard configuration
  if (!overlay_.empty() && config_.overlay.use_public_overlays) {
    std::vector<ton::ShardIdFull> shards;
    for (const auto& top : task->mc.tops) {
      shards.emplace_back(top.id.workchain, top.id.shard);
    }
    td::actor::send_closure(overlay_, &OverlaySource::update_shards, std::move(shards));
  }

  task->phase = Phase::PrevTops;
  start_prev_tops(*task);
}

void LibrarianNode::on_prev_tops_data(td::uint32 seqno, td::Result<td::BufferSlice> data) {
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::PrevTops) {
    return;
  }
  if (data.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to fetch prev mc block: " << data.move_as_error();
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<ParsedBlock> R) {
    td::actor::send_closure(SelfId, &LibrarianNode::on_prev_tops_parsed, seqno, std::move(R));
  });
  dispatch_parse_block(task->prev_tops_id, data.move_as_ok(), std::move(P));
  task->phase = Phase::ParsePrevTops;
}

void LibrarianNode::on_prev_tops_parsed(td::uint32 seqno, td::Result<ParsedBlock> parsed) {
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::ParsePrevTops) {
    return;
  }
  if (parsed.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to parse prev mc block: " << parsed.move_as_error();
    task->phase = Phase::PrevTops;
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  task->prev_tops_block = parsed.move_as_ok();
  for (const auto& top : task->prev_tops_block.tops) {
    task->prev_tops.insert(top.id);
  }
  task->have_prev_tops = true;
  task->phase = Phase::PrevTops;
  start_walk(*task);
}

void LibrarianNode::on_walk_data(td::uint32 seqno, ton::BlockIdExt id, td::Result<td::BufferSlice> data) {
  block_fetches_in_flight_--;
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::Walk) {
    return;
  }
  if (data.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to fetch block " << id.id.to_str() << ": "
                 << data.move_as_error();
    task->walk_queue.insert(task->walk_queue.begin(), id);
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno, id](td::Result<ParsedBlock> R) {
    td::actor::send_closure(SelfId, &LibrarianNode::on_walk_parsed, seqno, id, std::move(R));
  });
  dispatch_parse_block(id, data.move_as_ok(), std::move(P));
}

void LibrarianNode::on_walk_parsed(td::uint32 seqno, ton::BlockIdExt id, td::Result<ParsedBlock> parsed) {
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::Walk) {
    return;
  }
  if (parsed.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to parse block " << id.id.to_str() << ": "
                 << parsed.move_as_error();
    task->walk_queue.insert(task->walk_queue.begin(), id);
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  ParsedBlock b = parsed.move_as_ok();
  task->blocks.push_back(std::move(b));
  for (const auto& p : task->blocks.back().prev) {
    if (p.is_masterchain() || task->prev_tops.count(p.id) != 0) {
      continue;
    }
    if (task->visited.insert(p.id).second) {
      task->walk_queue.push_back(p);
      task->walk_outstanding++;
    }
  }
  CHECK(task->walk_outstanding > 0);
  task->walk_outstanding--;
  if (task->walk_outstanding == 0) {
    start_states(*task);
  } else {
    pump_all_walks();
  }
}

void LibrarianNode::on_state_data(td::uint32 seqno, td::uint32 st_idx, td::Result<td::BufferSlice> data) {
  state_fetches_in_flight_--;
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::States || st_idx >= task->state_tasks.size()) {
    return;
  }
  auto& st = task->state_tasks[st_idx];
  if (data.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to fetch state of " << account_str(st.account) << ": "
                 << data.move_as_error();
    st.fetching = false;
    if (++st.attempts >= kStateFetchMaxAttempts) {
      give_up_state(*task, st_idx);
      return;
    }
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), seqno, i = st_idx](td::Result<ParsedAccountState> R) {
        td::actor::send_closure(SelfId, &LibrarianNode::on_state_parsed, seqno, i, std::move(R));
      });
  td::actor::send_closure(workers_[next_worker_].get(), &ParseWorker::parse_account_state, st.account,
                          task->mc.gen_utime, data.move_as_ok(), config_.account_states.max_data_boc_size,
                          std::move(P));
  next_worker_ = (next_worker_ + 1) % workers_.size();
}

void LibrarianNode::give_up_state(SliceTask& task, td::uint32 st_idx) {
  auto& st = task.state_tasks[st_idx];
  LOG(WARNING) << "[librarian] slice " << task.seqno << ": giving up on state of " << account_str(st.account)
               << " after " << st.attempts << " attempts; committing without it";
  st.done = true;
  st.fetching = false;
  task.states_gave_up = true;
  CHECK(task.states_pending > 0);
  if (--task.states_pending == 0) {
    finish_slice(task);
  } else {
    pump_all_states();
  }
}

void LibrarianNode::on_state_parsed(td::uint32 seqno, td::uint32 st_idx, td::Result<ParsedAccountState> state) {
  SliceTask* task = find_task(seqno);
  if (task == nullptr || task->phase != Phase::States || st_idx >= task->state_tasks.size()) {
    return;
  }
  auto& st = task->state_tasks[st_idx];
  if (state.is_error()) {
    LOG(WARNING) << "[librarian] slice " << seqno << ": failed to parse state of " << account_str(st.account) << ": "
                 << state.move_as_error();
    st.fetching = false;
    if (++st.attempts >= kStateFetchMaxAttempts) {
      give_up_state(*task, st_idx);
      return;
    }
    task->retry_at = td::Timestamp::in(kFetchRetryInterval);
    return;
  }
  task->account_states.insert_or_assign(st.account, state.move_as_ok());
  st.done = true;
  st.fetching = false;
  CHECK(task->states_pending > 0);
  task->states_pending--;
  if (task->states_pending == 0) {
    finish_slice(*task);
  } else {
    pump_all_states();
  }
}

void LibrarianNode::on_chase(td::Result<ton::BlockIdExt> next) {
  chase_active_ = false;
  if (stopping_) {
    return;
  }
  if (next.is_error()) {
    // at tip (or peers unreachable): retry soon
    chase_retry_at_ = td::Timestamp::in(kChaseRetryInterval);
    return;
  }
  auto id = next.move_as_ok();
  mc_ids_[id.id.seqno] = id;
  tip_known_ = true;
  tip_seqno_ = std::max(tip_seqno_, id.id.seqno);
  chase_last_ = id;
  ensure_slices();
  // wake tasks that were waiting for this id
  for (auto& [seqno, task] : tasks_) {
    if (task.phase == Phase::NeedMcId) {
      resolve_mc_id(task);
    }
  }
  pump_chase();
}

void LibrarianNode::on_overlay_stats(td::Result<OverlaySource::Stats> stats) {
  if (stats.is_error()) {
    return;
  }
  auto s = stats.move_as_ok();
  LOG(INFO) << "[stats] overlay: fetched=" << s.blocks_fetched << " from-cache=" << s.blocks_from_cache
            << " failed=" << s.fetch_failures << " queries=" << s.queries_sent
            << " broadcasts=" << s.broadcasts_received << " (cached " << s.broadcasts_cached << ")";
}

void LibrarianNode::pump_chase() {
  if (stopping_ || chase_active_ || !chase_ready_) {
    return;
  }
  if (chase_retry_at_ && !chase_retry_at_.is_in_past()) {
    return;
  }
  chase_active_ = true;
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockIdExt> R) {
    td::actor::send_closure(SelfId, &LibrarianNode::on_chase, std::move(R));
  });
  td::actor::send_closure(overlay_, &OverlaySource::get_next_mc_block, chase_last_, std::move(P));
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

bool LibrarianNode::use_overlay_blocks() const {
  return config_.mode == Config::SyncMode::Overlay || config_.mode == Config::SyncMode::Hybrid;
}

void LibrarianNode::fetch_block_data(const ton::BlockIdExt& id, td::Promise<td::BufferSlice> promise) {
  if (use_overlay_blocks()) {
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), id, promise = std::move(promise)](td::Result<td::BufferSlice> R) mutable {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &LibrarianNode::fallback_fetch, id, R.move_as_error(),
                                    std::move(promise));
          } else {
            promise.set_value(R.move_as_ok());
          }
        });
    td::actor::send_closure(overlay_, &OverlaySource::get_block, id, std::move(P));
  } else {
    td::actor::send_closure(liteserver_, &LiteserverSource::get_block, id, std::move(promise));
  }
}

void LibrarianNode::fallback_fetch(ton::BlockIdExt id, td::Status error, td::Promise<td::BufferSlice> promise) {
  if (!liteserver_.empty()) {
    LOG(WARNING) << "[librarian] overlay fetch of " << id.id.to_str() << " failed (" << error
                 << "); falling back to liteserver";
    td::actor::send_closure(liteserver_, &LiteserverSource::get_block, id, std::move(promise));
  } else {
    promise.set_error(std::move(error));
  }
}

void LibrarianNode::dispatch_parse_block(const ton::BlockIdExt& id, td::BufferSlice data,
                                         td::Promise<ParsedBlock> promise) {
  td::actor::send_closure(workers_[next_worker_].get(), &ParseWorker::parse_block, id, std::move(data),
                          std::move(promise));
  next_worker_ = (next_worker_ + 1) % workers_.size();
}

LibrarianNode::SliceTask* LibrarianNode::find_task(td::uint32 seqno) {
  auto it = tasks_.find(seqno);
  return it == tasks_.end() ? nullptr : &it->second;
}

void LibrarianNode::log_stats() {
  auto p = storage_->get_progress();
  LOG(INFO) << "[stats] mc_seqno=" << p.mc_seqno << (tip_known_ ? PSTRING() << " tip=" << tip_seqno_
                                                                : std::string(" tip=?"))
            << " pending=" << next_commit_seqno_ << " slices-in-flight=" << tasks_.size()
            << " block-fetches=" << block_fetches_in_flight_ << " state-fetches=" << state_fetches_in_flight_
            << " total-blocks=" << p.total_blocks << " total-txs=" << p.total_txs << " total-msgs=" << p.total_msgs
            << " total-account-states=" << p.total_account_states << " total-slices=" << p.total_slices
            << " slice-errors=" << slice_errors_;
  if (!overlay_.empty()) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<OverlaySource::Stats> R) {
      td::actor::send_closure(SelfId, &LibrarianNode::on_overlay_stats, std::move(R));
    });
    td::actor::send_closure(overlay_, &OverlaySource::get_stats, std::move(P));
  }
}

void LibrarianNode::alarm() {
  if (stopping_) {
    return;
  }
  auto now = td::Timestamp::now();

  if (!liteserver_.empty() && (!next_tip_poll_ || next_tip_poll_.is_in_past())) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockIdExt> R) {
      td::actor::send_closure(SelfId, &LibrarianNode::on_tip, std::move(R));
    });
    td::actor::send_closure(liteserver_, &LiteserverSource::get_masterchain_info, std::move(P));
    next_tip_poll_ = now + kTipPollInterval;
  }

  if (chase_ready_ && !chase_active_ && (!chase_retry_at_ || chase_retry_at_.is_in_past())) {
    pump_chase();
  }

  for (auto& [seqno, task] : tasks_) {
    if (task.retry_at && task.retry_at.is_in_past()) {
      task.retry_at = td::Timestamp::never();
      task.attempts++;
      drive_task(task);
    }
  }

  maybe_finish();

  if (!next_stats_ || next_stats_.is_in_past()) {
    log_stats();
    next_stats_ = now + kStatsInterval;
  }

  td::Timestamp next = now + kHeartbeatInterval;
  next.relax(next_tip_poll_);
  if (chase_retry_at_) {
    next.relax(chase_retry_at_);
  }
  for (auto& [seqno, task] : tasks_) {
    next.relax(task.retry_at);
  }
  next.relax(next_stats_);
  alarm_timestamp().relax(next);
}

}  // namespace tonlibrarian
