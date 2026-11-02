/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <map>
#include <optional>
#include <set>
#include <vector>

#include "common.h"

namespace tonlibrarian {

// ---------------------------------------------------------------------------
// Parsed data structures (in-memory form, before RocksDB serialization).
// Mirrors the ton-indexer data model: blocks / transactions / messages /
// account states, with a masterchain slice as the atomic unit.
// ---------------------------------------------------------------------------

enum class MessageKind : td::uint8 { Int = 0, ExtIn = 1, ExtOut = 2 };
enum class AccountStatus : td::uint8 { Uninit = 0, Frozen = 1, Active = 2, Nonexist = 3 };

struct ParsedMessage {
  td::Bits256 hash;  // hash of the message cell
  MessageKind kind = MessageKind::Int;

  bool has_source = false;
  Account source;
  bool has_dest = false;
  Account dest;

  bool ihr_disabled = false, bounce = false, bounced = false;
  td::uint64 created_lt = 0;
  td::uint32 created_at = 0;

  Coins value;      // int_msg only
  Coins ihr_fee;    // int_msg (legacy: may hold extra_flags)
  Coins fwd_fee;    // int_msg
  Coins import_fee; // ext_in
  Coins extra_flags;  // int_msg, global version >= 12

  bool has_opcode = false;
  td::int32 opcode = 0;

  bool has_body = false;
  td::Bits256 body_hash{};
  td::uint64 body_bits = 0;
  td::BufferSlice body_boc;  // serialized body cell (BAG-of-cells), empty if no body

  bool has_init = false;
  td::Bits256 init_state_hash{};

  bool has_norm_hash = false;
  td::Bits256 norm_hash{};  // normalized hash (ext_in only)
};

struct ParsedPhases {
  // storage phase
  bool has_storage = false;
  Coins storage_fees_collected;
  Coins storage_fees_due;
  td::int8 storage_status_change = 0;

  // credit phase
  bool has_credit = false;
  Coins credit_due_fees;
  Coins credit;

  // compute phase
  bool compute_skipped = false;
  td::uint8 compute_skip_reason = 0;  // 0 no_state, 1 bad_state, 2 no_gas, 3 no_funds
  bool compute_success = false, compute_msg_state_used = false, compute_account_activated = false;
  Coins compute_gas_fees;
  td::uint64 compute_gas_used = 0, compute_gas_limit = 0;
  td::int64 compute_gas_credit = 0;
  td::int8 compute_mode = 0;
  td::int32 compute_exit_code = 0;
  bool compute_has_exit_arg = false;
  td::int64 compute_exit_arg = 0;
  td::uint32 compute_vm_steps = 0;
  td::Bits256 compute_vm_init_state_hash{}, compute_vm_final_state_hash{};

  // action phase
  bool has_action = false;
  bool action_success = false, action_valid = false, action_no_funds = false;
  td::int8 action_status_change = 0;
  Coins action_total_fwd_fees, action_total_action_fees;
  td::int32 action_result_code = 0;
  bool action_has_result_arg = false;
  td::int64 action_result_arg = 0;
  td::uint16 action_tot_actions = 0, action_spec_actions = 0, action_skipped_actions = 0,
             action_msgs_created = 0;
  td::Bits256 action_list_hash{};
  td::uint32 action_tot_msg_cells = 0, action_tot_msg_bits = 0;

  // bounce phase
  td::uint8 bounce_type = 0;  // 0 none, 1 negfunds, 2 nofunds, 3 ok
  td::uint32 bounce_msg_cells = 0, bounce_msg_bits = 0;
  Coins bounce_req_fwd_fees, bounce_msg_fees, bounce_fwd_fees;
};

struct ParsedTransaction {
  Account account;
  td::Bits256 hash{};
  td::uint64 lt = 0;
  td::uint32 now = 0;
  AccountStatus orig_status = AccountStatus::Uninit, end_status = AccountStatus::Uninit;
  Coins total_fees;
  td::Bits256 prev_trans_hash{};
  td::uint64 prev_trans_lt = 0;
  td::Bits256 state_hash_before{}, state_hash_after{};
  bool has_in_msg = false;
  td::Bits256 in_msg{};  // message cell hash
  std::vector<td::Bits256> out_msgs;

  td::uint8 descr_type = 0;  // 0 ord, 1 storage, 2 tick_tock, 3 split_prepare, 4 split_install,
                             // 5 merge_prepare, 6 merge_install
  bool aborted = false, destroyed = false, credit_first = false, is_tock = false;
  ParsedPhases phases;
  // SplitMergeInfo of trans_split_* / trans_merge_* descriptors.
  bool has_split_merge_info = false;
  td::uint8 sm_cur_shard_pfx_len = 0;  // 0..63
  td::uint8 sm_acc_split_depth = 0;    // 0..63
  td::Bits256 sm_this_addr{};
  td::Bits256 sm_sibling_addr{};
  td::Bits256 sm_prepare_tx_hash{};  // split_install / merge_install: prepare ^Transaction hash
  bool sm_installed = false;         // split_install: installed flag
};

struct ParsedBlock {
  ton::BlockIdExt id;

  // header
  td::int32 global_id = 0;
  td::uint32 version = 0;
  bool not_master = false, after_merge = false, before_split = false, after_split = false, want_merge = false,
       want_split = false, key_block = false, vert_seqno_incr = false;
  td::uint32 flags = 0;
  td::uint32 gen_utime = 0;
  td::uint64 start_lt = 0, end_lt = 0;
  td::uint32 gen_validator_list_hash_short = 0, gen_catchain_seqno = 0, min_ref_mc_seqno = 0,
             prev_key_block_seqno = 0, vert_seqno = 0;
  td::uint32 software_version = 0;
  td::uint64 software_capabilities = 0;

  std::vector<ton::BlockIdExt> prev;
  ton::BlockIdExt mc_blkid;  // for shard blocks: referenced masterchain block
  bool after_split_ = false;

  std::vector<ParsedTransaction> txs;  // ordered by (account, lt); index == position
  std::map<td::Bits256, ParsedMessage> messages;

  // masterchain only: shard top blocks from McBlockExtra.shard_hashes
  std::vector<ton::BlockIdExt> tops;
};

struct ParsedAccountState {
  Account account;
  AccountStatus status = AccountStatus::Nonexist;
  Coins balance;
  td::uint64 last_trans_lt = 0;
  td::Bits256 last_trans_hash{};
  td::Bits256 state_hash{};  // hash of the Account cell (0 for nonexist)
  td::Bits256 data_hash{}, code_hash{}, frozen_hash{};
  td::uint32 gen_utime = 0;
  td::BufferSlice data_boc, code_boc;  // may be empty (cap applied)
};

// Everything needed to atomically commit one masterchain slice.
struct SliceData {
  td::uint32 mc_seqno = 0;
  ton::BlockIdExt mc_id;
  td::uint32 mc_utime = 0;
  std::vector<ParsedBlock> blocks;     // all blocks of the slice diff (mc first)
  std::vector<ton::BlockIdExt> tops;   // full shard snapshot for this seqno (without masterchain)
  std::map<Account, ParsedAccountState> account_states;  // touched accounts (after-slice state)
  bool states_indexed = true;          // false if shard states were unavailable
};

// ---------------------------------------------------------------------------
// Parsing entry points.
// ---------------------------------------------------------------------------

// Parses a block BoC. Verifies root_hash and file_hash. For masterchain blocks
// also extracts shard tops from McBlockExtra.shard_hashes.
td::Result<ParsedBlock> parse_block(const ton::BlockIdExt& id, td::BufferSlice data);

// Parses an account state cell BoC (as returned by liteServer.getAccountState)
// into a ParsedAccountState of `account`. `gen_utime` is the timestamp of the
// masterchain block the state was resolved at. An empty BoC means a
// nonexistent account.
td::Result<ParsedAccountState> parse_account_state(td::BufferSlice boc, const Account& account,
                                                   td::uint32 gen_utime, size_t max_boc_size);

}  // namespace tonlibrarian
