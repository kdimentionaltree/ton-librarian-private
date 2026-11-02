/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <memory>
#include <optional>
#include <string>

#include "BlockParser.h"
#include "common.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
class WriteBatch;
class Slice;
}  // namespace rocksdb

namespace tonlibrarian {

struct Progress {
  td::uint32 mc_seqno = 0;
  td::Bits256 mc_root_hash{};
  td::Bits256 mc_file_hash{};
  td::uint32 mc_utime = 0;
  td::uint64 total_blocks = 0;
  td::uint64 total_txs = 0;
  td::uint64 total_msgs = 0;
  td::uint64 total_account_states = 0;
  td::uint64 total_slices = 0;
};

// Query results returned to the query tool.
struct BlockRecord {
  ton::BlockIdExt id;
  // from header
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
  td::uint32 mc_seqno = 0;  // masterchain seqno this block belongs to
  std::vector<ton::BlockIdExt> prev;
  ton::BlockIdExt mc_blkid;
  bool has_mc_blkid = false;
  td::uint32 tx_count = 0;
};

struct TxRecord : ParsedTransaction {
  // block location (wc/shard/seqno of the block containing the tx)
  ton::BlockId block;
};

struct MsgRecord : ParsedMessage {
};

struct AccountStateRecord : ParsedAccountState {
  td::uint32 mc_seqno = 0;
};

class Storage {
 public:
  Storage(std::string path, bool read_only = false);
  ~Storage();

  td::Status open();
  void close();

  // Atomically commits one masterchain slice. Must be called with strictly
  // increasing mc_seqno (mc_seqno == progress.mc_seqno + 1, or any value when
  // the database is empty).
  td::Status commit_slice(const SliceData& slice);

  Progress get_progress();

  // Returns shard top block ids stored for the given masterchain seqno
  // (the slice that was committed for it).
  std::vector<ton::BlockIdExt> load_prev_tops(td::uint32 mc_seqno);

  // The highest mc seqno with tops stored (for restarts).
  std::optional<td::uint32> get_last_tops_seqno();

  // ---- queries (for the query tool) ----
  std::optional<BlockRecord> get_block(const ton::BlockId& id);
  std::vector<td::Bits256> get_block_txs(const ton::BlockId& id);
  std::optional<TxRecord> get_tx(const td::Bits256& hash);
  std::vector<td::Bits256> get_account_txs(const Account& account, td::uint32 limit, bool reverse);
  std::optional<MsgRecord> get_msg(const td::Bits256& hash);
  std::optional<td::BufferSlice> get_msg_content(const td::Bits256& hash);
  struct MsgLink {
    td::Bits256 msg_hash;
    td::uint8 dir;  // 0 = out_msg of the tx, 1 = in_msg of the tx
    td::uint16 idx;
    td::Bits256 tx_hash;
  };
  std::vector<MsgLink> get_msg_txs(const td::Bits256& msg_hash);
  struct MsgAccountEntry {
    td::uint8 dir;  // 0 = account is source, 1 = account is dest
    td::uint64 created_lt;
    td::Bits256 msg_hash;
  };
  std::vector<MsgAccountEntry> get_account_msgs(const Account& account, td::uint32 limit, bool reverse);
  std::optional<AccountStateRecord> get_account_state(const Account& account);  // latest
  std::vector<AccountStateRecord> get_account_state_history(const Account& account, td::uint32 limit);

 private:
  std::string cf_name(size_t idx);
  rocksdb::ColumnFamilyHandle* cf(size_t idx);

  std::string path_;
  bool read_only_;
  std::unique_ptr<rocksdb::DB> db_;
  std::vector<rocksdb::ColumnFamilyHandle*> cfs_;
};

}  // namespace tonlibrarian
