/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "BlockParser.h"
#include "Config.h"
#include "LiteserverSource.h"
#include "OverlaySource.h"
#include "ParseWorker.h"
#include "Storage.h"
#include "common.h"
#include "td/actor/actor.h"
#include "td/utils/buffer.h"
#include "ton/ton-types.h"

namespace tonlibrarian {

// The coordinator actor. Fetches masterchain slices (mc block + all shard
// blocks produced under it), parses them on ParseWorker actors placed on
// dedicated threads and commits the slices into RocksDB strictly in
// mc_seqno order. Up to `pipeline.depth` slices are kept in flight.
class LibrarianNode : public td::actor::Actor {
 public:
  // `done` (optional) is set when the node finishes (until_seqno reached)
  // so that the main loop can exit.
  static td::actor::ActorOwn<LibrarianNode> create(Config config, std::atomic<bool>* done = nullptr);

  explicit LibrarianNode(Config config, std::atomic<bool>* done);

 private:
  enum class Phase {
    NeedMcId,       // resolving the mc block id (lookup / overlay chase)
    FetchMc,        // fetching mc block data
    ParseMc,        // mc block is being parsed
    PrevTops,       // resolving tops(S-1): from DB or by fetching mc block S-1
    ParsePrevTops,  // the bootstrap block S-1 is being parsed
    Walk,           // fetching + parsing the slice's shard blocks
    States,         // fetching + parsing account states of touched accounts
    Ready,          // assembled; waiting for its turn to be committed
  };

  struct StateTask {
    Account account;  // touched account; final state is resolved at the slice's mc block
    bool fetching = false;
    bool done = false;
    td::uint32 attempts = 0;  // fetch/parse retry rounds; bounded give-up
  };

  struct SliceTask {
    td::uint32 seqno = 0;
    Phase phase = Phase::NeedMcId;

    ton::BlockIdExt mc_id{};
    ParsedBlock mc;  // parsed mc block of this slice

    // gating set: BlockIds of tops(S-1)
    bool have_prev_tops = false;
    std::set<ton::BlockId> prev_tops;
    ton::BlockIdExt prev_tops_id{};  // bootstrap block (mc S-1), if used
    ParsedBlock prev_tops_block;

    // shard walk
    std::set<ton::BlockId> visited;
    std::vector<ton::BlockIdExt> walk_queue;  // to be fetched
    td::uint32 walk_outstanding = 0;          // queued + fetching + parsing
    std::vector<ParsedBlock> blocks;           // parsed shard blocks

    // account states of touched accounts
    std::vector<StateTask> state_tasks;
    td::uint32 states_pending = 0;
    bool states_gave_up = false;  // some account states were skipped after retries

    std::map<Account, ParsedAccountState> account_states;

    td::Timestamp retry_at{};  // when set and due, alarm() re-drives the task
    td::uint32 attempts = 0;  // whole-slice retry counter (for logging)
  };

  void start_up() override;
  void alarm() override;
  void tear_down() override;

  // --- setup ---
  void init_parse_workers();
  void on_overlay_init_block(td::Result<ton::BlockIdExt> init_block);

  // --- pipeline control ---
  void ensure_slices();
  void start_task(td::uint32 seqno);
  void drive_task(SliceTask& task);
  void resolve_mc_id(SliceTask& task);
  void fetch_mc(SliceTask& task);
  void start_prev_tops(SliceTask& task);
  void start_walk(SliceTask& task);
  void pump_walk(SliceTask& task);
  void start_states(SliceTask& task);
  void pump_states(SliceTask& task);
  void give_up_state(SliceTask& task, td::uint32 st_idx);
  void finish_slice(SliceTask& task);
  void try_commit();
  void maybe_finish();
  void finish(std::string reason);
  void pump_all_walks();
  void pump_all_states();
  void pump_chase();

  // --- async callbacks ---
  void on_tip(td::Result<ton::BlockIdExt> tip);
  void on_lookup(td::uint32 seqno, td::Result<ton::BlockIdExt> id);
  void on_mc_data(td::uint32 seqno, td::Result<td::BufferSlice> data);
  void on_mc_parsed(td::uint32 seqno, td::Result<ParsedBlock> parsed);
  void on_prev_tops_data(td::uint32 seqno, td::Result<td::BufferSlice> data);
  void on_prev_tops_parsed(td::uint32 seqno, td::Result<ParsedBlock> parsed);
  void on_walk_data(td::uint32 seqno, ton::BlockIdExt id, td::Result<td::BufferSlice> data);
  void on_walk_parsed(td::uint32 seqno, ton::BlockIdExt id, td::Result<ParsedBlock> parsed);
  void on_state_data(td::uint32 seqno, td::uint32 st_idx, td::Result<td::BufferSlice> data);
  void on_state_parsed(td::uint32 seqno, td::uint32 st_idx, td::Result<ParsedAccountState> state);
  void on_chase(td::Result<ton::BlockIdExt> next);
  void on_overlay_stats(td::Result<OverlaySource::Stats> stats);

  // --- helpers ---
  bool use_overlay_blocks() const;
  void fetch_block_data(const ton::BlockIdExt& id, td::Promise<td::BufferSlice> promise);
  void fallback_fetch(ton::BlockIdExt id, td::Status error, td::Promise<td::BufferSlice> promise);
  void dispatch_parse_block(const ton::BlockIdExt& id, td::BufferSlice data, td::Promise<ParsedBlock> promise);
  void log_stats();
  SliceTask* find_task(td::uint32 seqno);

  Config config_;
  std::atomic<bool>* done_ = nullptr;

  std::unique_ptr<Storage> storage_;
  td::actor::ActorOwn<LiteserverSource> liteserver_;
  td::actor::ActorOwn<OverlaySource> overlay_;
  std::vector<td::actor::ActorOwn<ParseWorker>> workers_;
  size_t next_worker_ = 0;

  // pipeline state
  std::map<td::uint32, SliceTask> tasks_;
  td::uint32 next_commit_seqno_ = 1;  // first slice not yet committed
  bool start_seqno_known_ = false;    // next_commit_seqno_ is meaningful
  bool start_at_tip_ = false;         // fresh db && from_seqno == 0
  bool tip_known_ = false;
  td::uint32 tip_seqno_ = 0;
  std::map<td::uint32, ton::BlockIdExt> mc_ids_;  // seqno -> mc BlockIdExt (chase/lookup cache)

  // overlay chase (overlay sync mode)
  ton::BlockIdExt chase_last_{};
  bool chase_ready_ = false;
  bool chase_active_ = false;
  td::Timestamp chase_retry_at_{};

  // timers
  td::Timestamp next_tip_poll_{};
  td::Timestamp next_stats_{};

  // global fetch caps (walk + states phases)
  td::uint32 block_fetches_in_flight_ = 0;
  td::uint32 state_fetches_in_flight_ = 0;

  // stats
  td::uint64 slice_errors_ = 0;
  td::Timestamp started_at_{};

  bool stopping_ = false;
};

}  // namespace tonlibrarian
