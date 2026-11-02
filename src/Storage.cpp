/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "Storage.h"

#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <array>
#include <cstring>
#include <set>

namespace tonlibrarian {

namespace {

enum CfIdx {
  CF_DEFAULT = 0,
  CF_META = 1,
  CF_BLOCKS = 2,
  CF_BLOCK_TXS = 3,
  CF_TXS = 4,
  CF_TX_BY_ACCOUNT = 5,
  CF_MESSAGES = 6,
  CF_MSG_BY_ACCOUNT = 7,
  CF_MSG_TXS = 8,
  CF_MSG_CONTENT = 9,
  CF_ACCOUNT_STATES = 10,
  CF_LATEST_ACCOUNTS = 11,
  CF_SHARD_STATES = 12,
  CF_COUNT = 13,
};

const std::array<const char*, CF_COUNT> kCfNames = {
    "default", "meta", "blocks", "block_txs", "txs", "tx_by_account", "messages", "msg_by_account",
    "msg_txs",  "msg_content", "account_states", "latest_accounts", "shard_states"};

td::Status to_status(const rocksdb::Status& s) {
  if (s.ok()) {
    return td::Status::OK();
  }
  return td::Status::Error(PSLICE() << "rocksdb: " << s.ToString());
}

rocksdb::Slice to_slice(const std::string& s) {
  return rocksdb::Slice(s.data(), s.size());
}

bool coins_present(const Coins& c) {
  return c.grams.not_null() && td::sgn(c.grams) != 0;
}

void put_block_id_ext(Writer& w, const ton::BlockIdExt& id) {
  w.i8(static_cast<td::int8>(id.id.workchain));
  w.u64(id.id.shard);
  w.u32(id.id.seqno);
  w.bits256(id.root_hash);
  w.bits256(id.file_hash);
}

ton::BlockIdExt read_block_id_ext(Reader& r) {
  auto wc = r.i8();
  auto shard = r.u64();
  auto seqno = r.u32();
  auto root = r.bits256();
  auto file = r.bits256();
  return ton::BlockIdExt{static_cast<ton::WorkchainId>(wc), shard, seqno, root, file};
}

void write_phases(Writer& w, const ParsedPhases& p) {
  td::uint8 mask = 0;
  if (p.has_storage) {
    mask |= 1;
  }
  if (p.has_credit) {
    mask |= 2;
  }
  if (p.compute_skipped) {
    mask |= 4;
  }
  if (p.has_action) {
    mask |= 8;
  }
  w.u8(mask);
  w.u8(p.bounce_type);
  if (p.has_storage) {
    p.storage_fees_collected.serialize(w);
    if (coins_present(p.storage_fees_due)) {
      w.boolean(true);
      p.storage_fees_due.serialize(w);
    } else {
      w.boolean(false);
    }
    w.i8(p.storage_status_change);
  }
  if (p.has_credit) {
    if (coins_present(p.credit_due_fees)) {
      w.boolean(true);
      p.credit_due_fees.serialize(w);
    } else {
      w.boolean(false);
    }
    p.credit.serialize(w);
  }
  if (p.compute_skipped) {
    w.u8(p.compute_skip_reason);
  } else {
    w.boolean(p.compute_success);
    w.boolean(p.compute_msg_state_used);
    w.boolean(p.compute_account_activated);
    p.compute_gas_fees.serialize(w);
    w.u64(p.compute_gas_used);
    w.u64(p.compute_gas_limit);
    if (p.compute_gas_credit != 0) {
      w.boolean(true);
      w.i64(p.compute_gas_credit);
    } else {
      w.boolean(false);
    }
    w.i8(p.compute_mode);
    w.i32(p.compute_exit_code);
    w.boolean(p.compute_has_exit_arg);
    if (p.compute_has_exit_arg) {
      w.i64(p.compute_exit_arg);
    }
    w.u32(p.compute_vm_steps);
    w.bits256(p.compute_vm_init_state_hash);
    w.bits256(p.compute_vm_final_state_hash);
  }
  if (p.has_action) {
    w.boolean(p.action_success);
    w.boolean(p.action_valid);
    w.boolean(p.action_no_funds);
    w.i8(p.action_status_change);
    if (coins_present(p.action_total_fwd_fees)) {
      w.boolean(true);
      p.action_total_fwd_fees.serialize(w);
    } else {
      w.boolean(false);
    }
    if (coins_present(p.action_total_action_fees)) {
      w.boolean(true);
      p.action_total_action_fees.serialize(w);
    } else {
      w.boolean(false);
    }
    w.i32(p.action_result_code);
    w.boolean(p.action_has_result_arg);
    if (p.action_has_result_arg) {
      w.i64(p.action_result_arg);
    }
    w.u16(p.action_tot_actions);
    w.u16(p.action_spec_actions);
    w.u16(p.action_skipped_actions);
    w.u16(p.action_msgs_created);
    w.bits256(p.action_list_hash);
    w.u32(p.action_tot_msg_cells);
    w.u32(p.action_tot_msg_bits);
  }
  if (p.bounce_type != 0) {
    w.u32(p.bounce_msg_cells);
    w.u32(p.bounce_msg_bits);
    if (p.bounce_type == 2) {
      p.bounce_req_fwd_fees.serialize(w);
    } else if (p.bounce_type == 3) {
      p.bounce_msg_fees.serialize(w);
      p.bounce_fwd_fees.serialize(w);
    }
  }
}

ParsedPhases read_phases(Reader& r) {
  ParsedPhases p;
  td::uint8 mask = r.u8();
  p.has_storage = mask & 1;
  p.has_credit = mask & 2;
  p.compute_skipped = mask & 4;
  p.has_action = mask & 8;
  p.bounce_type = r.u8();
  if (p.has_storage) {
    p.storage_fees_collected = Coins::deserialize(r);
    if (r.boolean()) {
      p.storage_fees_due = Coins::deserialize(r);
    }
    p.storage_status_change = r.i8();
  }
  if (p.has_credit) {
    if (r.boolean()) {
      p.credit_due_fees = Coins::deserialize(r);
    }
    p.credit = Coins::deserialize(r);
  }
  if (p.compute_skipped) {
    p.compute_skip_reason = r.u8();
  } else {
    p.compute_success = r.boolean();
    p.compute_msg_state_used = r.boolean();
    p.compute_account_activated = r.boolean();
    p.compute_gas_fees = Coins::deserialize(r);
    p.compute_gas_used = r.u64();
    p.compute_gas_limit = r.u64();
    if (r.boolean()) {
      p.compute_gas_credit = r.i64();
    }
    p.compute_mode = r.i8();
    p.compute_exit_code = r.i32();
    p.compute_has_exit_arg = r.boolean();
    if (p.compute_has_exit_arg) {
      p.compute_exit_arg = r.i64();
    }
    p.compute_vm_steps = r.u32();
    p.compute_vm_init_state_hash = r.bits256();
    p.compute_vm_final_state_hash = r.bits256();
  }
  if (p.has_action) {
    p.action_success = r.boolean();
    p.action_valid = r.boolean();
    p.action_no_funds = r.boolean();
    p.action_status_change = r.i8();
    if (r.boolean()) {
      p.action_total_fwd_fees = Coins::deserialize(r);
    }
    if (r.boolean()) {
      p.action_total_action_fees = Coins::deserialize(r);
    }
    p.action_result_code = r.i32();
    p.action_has_result_arg = r.boolean();
    if (p.action_has_result_arg) {
      p.action_result_arg = r.i64();
    }
    p.action_tot_actions = r.u16();
    p.action_spec_actions = r.u16();
    p.action_skipped_actions = r.u16();
    p.action_msgs_created = r.u16();
    p.action_list_hash = r.bits256();
    p.action_tot_msg_cells = r.u32();
    p.action_tot_msg_bits = r.u32();
  }
  if (p.bounce_type != 0) {
    p.bounce_msg_cells = r.u32();
    p.bounce_msg_bits = r.u32();
    if (p.bounce_type == 2) {
      p.bounce_req_fwd_fees = Coins::deserialize(r);
    } else if (p.bounce_type == 3) {
      p.bounce_msg_fees = Coins::deserialize(r);
      p.bounce_fwd_fees = Coins::deserialize(r);
    }
  }
  return p;
}

std::string make_block_record(const ParsedBlock& b) {
  Writer w;
  w.bits256(b.id.root_hash);
  w.bits256(b.id.file_hash);
  w.i32(b.global_id);
  w.u32(b.version);
  td::uint8 flags = 0;
  if (b.not_master) {
    flags |= 1;
  }
  if (b.after_merge) {
    flags |= 2;
  }
  if (b.before_split) {
    flags |= 4;
  }
  if (b.after_split) {
    flags |= 8;
  }
  if (b.want_merge) {
    flags |= 16;
  }
  if (b.want_split) {
    flags |= 32;
  }
  if (b.key_block) {
    flags |= 64;
  }
  if (b.vert_seqno_incr) {
    flags |= 128;
  }
  w.u8(flags);
  w.u8(static_cast<td::uint8>(b.flags & 0xff));
  w.u32(b.gen_utime);
  w.u64(b.start_lt);
  w.u64(b.end_lt);
  w.u32(b.gen_validator_list_hash_short);
  w.u32(b.gen_catchain_seqno);
  w.u32(b.min_ref_mc_seqno);
  w.u32(b.prev_key_block_seqno);
  w.u32(b.vert_seqno);
  w.u32(b.software_version);
  w.u64(b.software_capabilities);
  w.u32(b.id.is_masterchain() ? b.id.id.seqno : b.mc_blkid.id.seqno);
  w.u8(static_cast<td::uint8>(b.prev.size()));
  for (const auto& prev : b.prev) {
    put_block_id_ext(w, prev);
  }
  if (b.id.is_masterchain()) {
    w.boolean(false);
  } else {
    w.boolean(true);
    put_block_id_ext(w, b.mc_blkid);
  }
  w.boolean(b.after_split_);
  w.u32(static_cast<td::uint32>(b.txs.size()));
  return w.take();
}

std::string make_tx_record(const ParsedTransaction& t, const ton::BlockId& block) {
  Writer w;
  w.i8(t.account.wc);
  w.raw(t.account.addr.as_slice());
  w.u64(t.lt);
  w.u32(t.now);
  w.u8(static_cast<td::uint8>(t.orig_status));
  w.u8(static_cast<td::uint8>(t.end_status));
  t.total_fees.serialize(w);
  w.bits256(t.prev_trans_hash);
  w.u64(t.prev_trans_lt);
  w.bits256(t.state_hash_before);
  w.bits256(t.state_hash_after);
  w.u8(t.descr_type);
  td::uint8 descr_flags = 0;
  if (t.aborted) {
    descr_flags |= 1;
  }
  if (t.destroyed) {
    descr_flags |= 2;
  }
  if (t.credit_first) {
    descr_flags |= 4;
  }
  if (t.is_tock) {
    descr_flags |= 8;
  }
  w.u8(descr_flags);
  w.boolean(t.has_in_msg);
  if (t.has_in_msg) {
    w.bits256(t.in_msg);
  }
  w.u16(static_cast<td::uint16>(t.out_msgs.size()));
  for (const auto& out : t.out_msgs) {
    w.bits256(out);
  }
  w.i8(static_cast<td::int8>(block.workchain));
  w.u64(block.shard);
  w.u32(block.seqno);
  write_phases(w, t.phases);
  w.boolean(t.has_split_merge_info);
  if (t.has_split_merge_info) {
    w.u8(t.sm_cur_shard_pfx_len);
    w.u8(t.sm_acc_split_depth);
    w.bits256(t.sm_this_addr);
    w.bits256(t.sm_sibling_addr);
    w.bits256(t.sm_prepare_tx_hash);
    w.boolean(t.sm_installed);
  }
  return w.take();
}

std::string make_msg_record(const ParsedMessage& m) {
  Writer w;
  w.u8(static_cast<td::uint8>(m.kind));
  w.boolean(m.has_source);
  if (m.has_source) {
    w.i8(m.source.wc);
    w.raw(m.source.addr.as_slice());
  }
  w.boolean(m.has_dest);
  if (m.has_dest) {
    w.i8(m.dest.wc);
    w.raw(m.dest.addr.as_slice());
  }
  w.boolean(m.ihr_disabled);
  w.boolean(m.bounce);
  w.boolean(m.bounced);
  w.u64(m.created_lt);
  w.u32(m.created_at);
  m.value.serialize(w);
  m.import_fee.serialize(w);
  m.fwd_fee.serialize(w);
  m.extra_flags.serialize(w);
  w.boolean(m.has_opcode);
  if (m.has_opcode) {
    w.i32(m.opcode);
  }
  w.boolean(m.has_init);
  if (m.has_init) {
    w.bits256(m.init_state_hash);
  }
  w.boolean(m.has_norm_hash);
  if (m.has_norm_hash) {
    w.bits256(m.norm_hash);
  }
  w.boolean(m.has_body);
  if (m.has_body) {
    w.bits256(m.body_hash);
    w.u64(m.body_bits);
  }
  return w.take();
}

std::string make_account_state_record(const ParsedAccountState& st, td::uint32 mc_seqno) {
  Writer w;
  w.u8(static_cast<td::uint8>(st.status));
  st.balance.serialize(w);
  w.bits256(st.last_trans_hash);
  w.u64(st.last_trans_lt);
  w.bits256(st.state_hash);
  w.bits256(st.data_hash);
  w.bits256(st.code_hash);
  w.bits256(st.frozen_hash);
  w.u32(st.gen_utime);
  w.u32(mc_seqno);
  w.varbytes(st.data_boc.as_slice());
  w.varbytes(st.code_boc.as_slice());
  return w.take();
}

std::string make_progress_record(const Progress& p) {
  Writer w;
  w.u32(p.mc_seqno);
  w.bits256(p.mc_root_hash);
  w.bits256(p.mc_file_hash);
  w.u32(p.mc_utime);
  w.varu64(p.total_blocks);
  w.varu64(p.total_txs);
  w.varu64(p.total_msgs);
  w.varu64(p.total_account_states);
  w.varu64(p.total_slices);
  return w.take();
}

Progress read_progress_record(td::Slice data) {
  Progress p;
  Reader r{data};
  p.mc_seqno = r.u32();
  p.mc_root_hash = r.bits256();
  p.mc_file_hash = r.bits256();
  p.mc_utime = r.u32();
  p.total_blocks = r.varu64();
  p.total_txs = r.varu64();
  p.total_msgs = r.varu64();
  p.total_account_states = r.varu64();
  p.total_slices = r.varu64();
  return p;
}

// Reads last_trans_lt out of an account-state record (cheap prefix parse).
td::uint64 account_state_record_lt(td::Slice data) {
  Reader r{data};
  r.u8();                            // status
  Coins::deserialize(r);             // balance
  r.bits256();                       // last_trans_hash
  return r.u64();                    // last_trans_lt
}

AccountStateRecord read_account_state_record(td::Slice data) {
  Reader r{data};
  AccountStateRecord st;
  st.status = static_cast<AccountStatus>(r.u8());
  st.balance = Coins::deserialize(r);
  st.last_trans_hash = r.bits256();
  st.last_trans_lt = r.u64();
  st.state_hash = r.bits256();
  st.data_hash = r.bits256();
  st.code_hash = r.bits256();
  st.frozen_hash = r.bits256();
  st.gen_utime = r.u32();
  st.mc_seqno = r.u32();
  st.data_boc = td::BufferSlice{r.varbytes()};
  st.code_boc = td::BufferSlice{r.varbytes()};
  return st;
}

}  // namespace

Storage::Storage(std::string path, bool read_only) : path_(std::move(path)), read_only_(read_only) {
}

Storage::~Storage() {
  close();
}

td::Status Storage::open() {
  rocksdb::DBOptions db_opts;
  db_opts.create_if_missing = true;
  db_opts.create_missing_column_families = true;
  db_opts.max_background_jobs = 8;
  db_opts.db_write_buffer_size = 64 << 20;
  db_opts.keep_log_file_num = 2;

  auto shared_cache = rocksdb::NewLRUCache(512ULL << 20);
  rocksdb::ColumnFamilyOptions cf_opts;
  cf_opts.level_compaction_dynamic_level_bytes = true;
  cf_opts.OptimizeLevelStyleCompaction(256ULL << 20);
  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.block_size = 32 << 10;
  table_opts.format_version = 5;
  table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_opts.block_cache = shared_cache;
  cf_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

  std::vector<rocksdb::ColumnFamilyDescriptor> desc;
  for (int i = 0; i < CF_COUNT; i++) {
    desc.emplace_back(kCfNames[i], cf_opts);
  }

  rocksdb::DB* db = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::Status s;
  if (read_only_) {
    s = rocksdb::DB::OpenForReadOnly(db_opts, path_, desc, &handles, &db, false);
  } else {
    s = rocksdb::DB::Open(db_opts, path_, desc, &handles, &db);
  }
  TRY_STATUS(to_status(s));
  db_ = std::unique_ptr<rocksdb::DB>(db);
  cfs_ = std::move(handles);
  return td::Status::OK();
}

void Storage::close() {
  if (db_) {
    for (auto* h : cfs_) {
      if (h) {
        db_->DestroyColumnFamilyHandle(h);
      }
    }
    cfs_.clear();
    db_.reset();
  }
}

rocksdb::ColumnFamilyHandle* Storage::cf(size_t idx) {
  return cfs_[idx];
}

std::string Storage::cf_name(size_t idx) {
  return kCfNames[idx];
}

Progress Storage::get_progress() {
  Progress p;
  if (!db_) {
    return p;
  }
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_META), to_slice("progress"), &value);
  if (s.ok()) {
    p = read_progress_record(value);
  }
  return p;
}

td::Status Storage::commit_slice(const SliceData& slice) {
  if (!db_) {
    return td::Status::Error("storage is not open");
  }
  Progress prog = get_progress();
  if (slice.mc_seqno == 0) {
    return td::Status::Error("attempt to commit masterchain seqno 0");
  }
  if (prog.mc_seqno != 0) {
    if (slice.mc_seqno != prog.mc_seqno + 1) {
      return td::Status::Error(PSLICE() << "non-sequential slice: have " << prog.mc_seqno << ", got "
                                         << slice.mc_seqno);
    }
  }

  rocksdb::WriteBatch batch;
  std::set<td::Bits256> slice_messages;
  td::uint64 slice_txs = 0;

  for (const auto& b : slice.blocks) {
    if (b.id.is_masterchain() && b.id.id.seqno != slice.mc_seqno) {
      return td::Status::Error(PSLICE() << "slice " << slice.mc_seqno << " contains mc block "
                                         << b.id.id.seqno);
    }
    batch.Put(cf(CF_BLOCKS), to_slice(block_key(b.id)), to_slice(make_block_record(b)));
    for (size_t i = 0; i < b.txs.size(); i++) {
      const auto& tx = b.txs[i];
      batch.Put(cf(CF_BLOCK_TXS), to_slice(block_tx_key(b.id, static_cast<td::uint32>(i))),
                to_slice(tx.hash.as_slice().str()));
      batch.Put(cf(CF_TXS), to_slice(tx.hash.as_slice().str()),
                to_slice(make_tx_record(tx, b.id.id)));
      batch.Put(cf(CF_TX_BY_ACCOUNT), to_slice(tx_by_account_key(tx.account.wc, tx.account.addr.as_slice(), tx.lt)),
                to_slice(tx.hash.as_slice().str()));
      if (tx.has_in_msg) {
        batch.Put(cf(CF_MSG_TXS), to_slice(msg_link_key(tx.in_msg, 1, 0)),
                  to_slice(tx.hash.as_slice().str()));
      }
      for (size_t j = 0; j < tx.out_msgs.size(); j++) {
        batch.Put(cf(CF_MSG_TXS), to_slice(msg_link_key(tx.out_msgs[j], 0, static_cast<td::uint16>(j))),
                  to_slice(tx.hash.as_slice().str()));
      }
      slice_txs++;
    }
    for (const auto& [hash, msg] : b.messages) {
      if (slice_messages.insert(hash).second) {
        batch.Put(cf(CF_MESSAGES), to_slice(hash.as_slice().str()), to_slice(make_msg_record(msg)));
        if (msg.has_body && msg.body_boc.size() > 0) {
          batch.Put(cf(CF_MSG_CONTENT), to_slice(hash.as_slice().str()),
                    to_slice(msg.body_boc.as_slice().str()));
        }
      }
      if (msg.created_lt > 0) {
        if (msg.has_source) {
          batch.Put(cf(CF_MSG_BY_ACCOUNT),
                    to_slice(msg_by_account_key(msg.source.wc, msg.source.addr.as_slice(), 0, msg.created_lt)),
                    to_slice(hash.as_slice().str()));
        }
        if (msg.has_dest) {
          batch.Put(cf(CF_MSG_BY_ACCOUNT),
                    to_slice(msg_by_account_key(msg.dest.wc, msg.dest.addr.as_slice(), 1, msg.created_lt)),
                    to_slice(hash.as_slice().str()));
        }
      }
    }
  }

  for (const auto& top : slice.tops) {
    Writer w;
    w.u32(top.id.seqno);
    w.bits256(top.root_hash);
    w.bits256(top.file_hash);
    batch.Put(cf(CF_SHARD_STATES),
              to_slice(shard_state_key(slice.mc_seqno, top.id.workchain, top.id.shard)), to_slice(w.take()));
  }

  for (const auto& [account, st] : slice.account_states) {
    std::string hist_key = tx_by_account_key(account.wc, account.addr.as_slice(), st.last_trans_lt);
    batch.Put(cf(CF_ACCOUNT_STATES), to_slice(hist_key),
              to_slice(make_account_state_record(st, slice.mc_seqno)));
    std::string existing;
    auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_LATEST_ACCOUNTS), to_slice(account.key()), &existing);
    if (!s.ok() || account_state_record_lt(existing) <= st.last_trans_lt) {
      batch.Put(cf(CF_LATEST_ACCOUNTS), to_slice(account.key()),
                to_slice(make_account_state_record(st, slice.mc_seqno)));
    }
  }

  prog.mc_seqno = slice.mc_seqno;
  prog.mc_root_hash = slice.mc_id.root_hash;
  prog.mc_file_hash = slice.mc_id.file_hash;
  prog.mc_utime = slice.mc_utime;
  prog.total_blocks += slice.blocks.size();
  prog.total_txs += slice_txs;
  prog.total_msgs += slice_messages.size();
  prog.total_account_states += slice.account_states.size();
  prog.total_slices++;
  batch.Put(cf(CF_META), to_slice("progress"), to_slice(make_progress_record(prog)));

  auto s = db_->Write(rocksdb::WriteOptions(), &batch);
  TRY_STATUS(to_status(s));
  return td::Status::OK();
}

std::vector<ton::BlockIdExt> Storage::load_prev_tops(td::uint32 mc_seqno) {
  std::vector<ton::BlockIdExt> res;
  if (!db_) {
    return res;
  }
  std::string prefix = shard_state_prefix(mc_seqno);
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_SHARD_STATES))};
  for (it->Seek(to_slice(prefix)); it->Valid() && it->key().size() >= 13 &&
                                  std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0;
       it->Next()) {
    try {
      // key layout: mc_seqno (u32 be) + workchain (i8) + shard (u64 be);
      // value layout: shard block seqno (u32) + root_hash + file_hash
      auto key = it->key();
      td::int8 wc = static_cast<td::int8>(key.data()[4]);
      td::uint64 shard = 0;
      for (int i = 5; i < 13; i++) {
        shard = (shard << 8) | static_cast<td::uint8>(key.data()[i]);
      }
      Reader r{td::Slice{it->value().data(), it->value().size()}};
      td::uint32 seqno = r.u32();
      td::Bits256 root = r.bits256();
      td::Bits256 file = r.bits256();
      res.push_back(ton::BlockIdExt{static_cast<ton::WorkchainId>(wc), shard, seqno, root, file});
    } catch (const SerError&) {
      // malformed record: skip instead of aborting the node
    }
  }
  return res;
}

std::optional<td::uint32> Storage::get_last_tops_seqno() {
  if (!db_) {
    return std::nullopt;
  }
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_SHARD_STATES))};
  it->SeekToLast();
  if (!it->Valid() || it->key().size() < 4) {
    return std::nullopt;
  }
  td::uint32 seqno = 0;
  for (int i = 0; i < 4; i++) {
    seqno = (seqno << 8) | static_cast<td::uint8>(it->key().data()[i]);
  }
  return seqno;
}

std::optional<BlockRecord> Storage::get_block(const ton::BlockId& id) {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_BLOCKS),
                    to_slice(block_key(id.workchain, id.shard, id.seqno)), &value);
  if (!s.ok()) {
    return std::nullopt;
  }
  Reader r{value};
  BlockRecord b;
  b.id = ton::BlockIdExt{id.workchain, id.shard, id.seqno, r.bits256(), r.bits256()};
  b.global_id = r.i32();
  b.version = r.u32();
  td::uint8 flags = r.u8();
  b.not_master = flags & 1;
  b.after_merge = flags & 2;
  b.before_split = flags & 4;
  b.after_split = flags & 8;
  b.want_merge = flags & 16;
  b.want_split = flags & 32;
  b.key_block = flags & 64;
  b.vert_seqno_incr = flags & 128;
  b.flags = r.u8();
  b.gen_utime = r.u32();
  b.start_lt = r.u64();
  b.end_lt = r.u64();
  b.gen_validator_list_hash_short = r.u32();
  b.gen_catchain_seqno = r.u32();
  b.min_ref_mc_seqno = r.u32();
  b.prev_key_block_seqno = r.u32();
  b.vert_seqno = r.u32();
  b.software_version = r.u32();
  b.software_capabilities = r.u64();
  b.mc_seqno = r.u32();
  auto n_prev = r.u8();
  for (int i = 0; i < n_prev; i++) {
    b.prev.push_back(read_block_id_ext(r));
  }
  if (r.boolean()) {
    b.mc_blkid = read_block_id_ext(r);
    b.has_mc_blkid = true;
  }
  b.after_split = r.boolean() || b.after_split;
  b.tx_count = r.u32();
  return b;
}

std::vector<td::Bits256> Storage::get_block_txs(const ton::BlockId& id) {
  std::vector<td::Bits256> res;
  std::string prefix = block_key(id.workchain, id.shard, id.seqno);
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_BLOCK_TXS))};
  for (it->Seek(to_slice(prefix)); it->Valid() && it->key().size() == prefix.size() + 4 &&
                                  std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0;
       it->Next()) {
    td::Bits256 h;
    h.as_slice().copy_from(td::Slice{it->value().data(), 32});
    res.push_back(h);
  }
  return res;
}

std::optional<TxRecord> Storage::get_tx(const td::Bits256& hash) {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_TXS), to_slice(hash.as_slice().str()), &value);
  if (!s.ok()) {
    return std::nullopt;
  }
  Reader r{value};
  TxRecord t;
  t.hash = hash;  // the key is the hash; the record itself does not store it
  t.account.wc = r.i8();
  t.account.addr = r.bits256();
  t.lt = r.u64();
  t.now = r.u32();
  t.orig_status = static_cast<AccountStatus>(r.u8());
  t.end_status = static_cast<AccountStatus>(r.u8());
  t.total_fees = Coins::deserialize(r);
  t.prev_trans_hash = r.bits256();
  t.prev_trans_lt = r.u64();
  t.state_hash_before = r.bits256();
  t.state_hash_after = r.bits256();
  t.descr_type = r.u8();
  td::uint8 descr_flags = r.u8();
  t.aborted = descr_flags & 1;
  t.destroyed = descr_flags & 2;
  t.credit_first = descr_flags & 4;
  t.is_tock = descr_flags & 8;
  t.has_in_msg = r.boolean();
  if (t.has_in_msg) {
    t.in_msg = r.bits256();
  }
  auto n_out = r.u16();
  for (int i = 0; i < n_out; i++) {
    t.out_msgs.push_back(r.bits256());
  }
  auto wc = r.i8();
  auto shard = r.u64();
  auto seqno = r.u32();
  t.block = ton::BlockId{static_cast<ton::WorkchainId>(wc), shard, seqno};
  t.phases = read_phases(r);
  t.has_split_merge_info = r.boolean();
  if (t.has_split_merge_info) {
    t.sm_cur_shard_pfx_len = r.u8();
    t.sm_acc_split_depth = r.u8();
    t.sm_this_addr = r.bits256();
    t.sm_sibling_addr = r.bits256();
    t.sm_prepare_tx_hash = r.bits256();
    t.sm_installed = r.boolean();
  }
  return t;
}

std::vector<td::Bits256> Storage::get_account_txs(const Account& account, td::uint32 limit, bool reverse) {
  std::vector<td::Bits256> res;
  std::string prefix = account.key();
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_TX_BY_ACCOUNT))};
  if (reverse) {
    std::string start = prefix + std::string(8, '\xff');
    for (it->SeekForPrev(to_slice(start)); it->Valid() && it->key().size() == prefix.size() + 8 &&
                                            std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0 &&
                                            res.size() < limit;
         it->Prev()) {
      td::Bits256 h;
      h.as_slice().copy_from(td::Slice{it->value().data(), 32});
      res.push_back(h);
    }
  } else {
    for (it->Seek(to_slice(prefix)); it->Valid() && it->key().size() == prefix.size() + 8 &&
                                    std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0 &&
                                    res.size() < limit;
         it->Next()) {
      td::Bits256 h;
      h.as_slice().copy_from(td::Slice{it->value().data(), 32});
      res.push_back(h);
    }
  }
  return res;
}

std::optional<MsgRecord> Storage::get_msg(const td::Bits256& hash) {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_MESSAGES), to_slice(hash.as_slice().str()), &value);
  if (!s.ok()) {
    return std::nullopt;
  }
  Reader r{value};
  MsgRecord m;
  m.hash = hash;  // the key is the hash; the record itself does not store it
  m.kind = static_cast<MessageKind>(r.u8());
  m.has_source = r.boolean();
  if (m.has_source) {
    m.source.wc = r.i8();
    m.source.addr = r.bits256();
  }
  m.has_dest = r.boolean();
  if (m.has_dest) {
    m.dest.wc = r.i8();
    m.dest.addr = r.bits256();
  }
  m.ihr_disabled = r.boolean();
  m.bounce = r.boolean();
  m.bounced = r.boolean();
  m.created_lt = r.u64();
  m.created_at = r.u32();
  m.value = Coins::deserialize(r);
  m.import_fee = Coins::deserialize(r);
  m.fwd_fee = Coins::deserialize(r);
  m.extra_flags = Coins::deserialize(r);
  m.has_opcode = r.boolean();
  if (m.has_opcode) {
    m.opcode = r.i32();
  }
  m.has_init = r.boolean();
  if (m.has_init) {
    m.init_state_hash = r.bits256();
  }
  m.has_norm_hash = r.boolean();
  if (m.has_norm_hash) {
    m.norm_hash = r.bits256();
  }
  m.has_body = r.boolean();
  if (m.has_body) {
    m.body_hash = r.bits256();
    m.body_bits = r.u64();
  }
  return m;
}

std::optional<td::BufferSlice> Storage::get_msg_content(const td::Bits256& hash) {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_MSG_CONTENT), to_slice(hash.as_slice().str()), &value);
  if (!s.ok()) {
    return std::nullopt;
  }
  return td::BufferSlice{value};
}

std::vector<Storage::MsgLink> Storage::get_msg_txs(const td::Bits256& msg_hash) {
  std::vector<MsgLink> res;
  std::string prefix = msg_hash.as_slice().str();
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_MSG_TXS))};
  for (it->Seek(to_slice(prefix)); it->Valid() && it->key().size() == prefix.size() + 3 &&
                                  std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0;
       it->Next()) {
    MsgLink link;
    link.msg_hash = msg_hash;
    link.dir = static_cast<td::uint8>(it->key().data()[prefix.size()]);
    link.idx = (static_cast<td::uint16>(static_cast<td::uint8>(it->key().data()[prefix.size() + 1])) << 8) |
               static_cast<td::uint8>(it->key().data()[prefix.size() + 2]);
    link.tx_hash.as_slice().copy_from(td::Slice{it->value().data(), 32});
    res.push_back(link);
  }
  return res;
}

std::vector<Storage::MsgAccountEntry> Storage::get_account_msgs(const Account& account, td::uint32 limit,
                                                              bool reverse) {
  std::vector<MsgAccountEntry> res;
  std::string prefix = account.key();
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_MSG_BY_ACCOUNT))};
  if (reverse) {
    std::string start = prefix + std::string(9, '\xff');
    for (it->SeekForPrev(to_slice(start)); it->Valid() && it->key().size() == prefix.size() + 9 &&
                                            std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0 &&
                                            res.size() < limit;
         it->Prev()) {
      MsgAccountEntry e;
      e.dir = static_cast<td::uint8>(it->key().data()[prefix.size()]);
      const char* lt_data = it->key().data() + prefix.size() + 1;
      e.created_lt = 0;
      for (int i = 0; i < 8; i++) {
        e.created_lt = (e.created_lt << 8) | static_cast<td::uint8>(lt_data[i]);
      }
      e.msg_hash.as_slice().copy_from(td::Slice{it->value().data(), 32});
      res.push_back(e);
    }
  } else {
    for (it->Seek(to_slice(prefix)); it->Valid() && it->key().size() == prefix.size() + 9 &&
                                    std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0 &&
                                    res.size() < limit;
         it->Next()) {
      MsgAccountEntry e;
      e.dir = static_cast<td::uint8>(it->key().data()[prefix.size()]);
      const char* lt_data = it->key().data() + prefix.size() + 1;
      e.created_lt = 0;
      for (int i = 0; i < 8; i++) {
        e.created_lt = (e.created_lt << 8) | static_cast<td::uint8>(lt_data[i]);
      }
      e.msg_hash.as_slice().copy_from(td::Slice{it->value().data(), 32});
      res.push_back(e);
    }
  }
  return res;
}

std::optional<AccountStateRecord> Storage::get_account_state(const Account& account) {
  std::string value;
  auto s = db_->Get(rocksdb::ReadOptions(), cf(CF_LATEST_ACCOUNTS), to_slice(account.key()), &value);
  if (!s.ok()) {
    return std::nullopt;
  }
  auto rec = read_account_state_record(value);
  rec.account = account;  // the key carries the identity; the value does not
  return rec;
}

std::vector<AccountStateRecord> Storage::get_account_state_history(const Account& account, td::uint32 limit) {
  std::vector<AccountStateRecord> res;
  std::string prefix = account.key();
  std::unique_ptr<rocksdb::Iterator> it{db_->NewIterator(rocksdb::ReadOptions(), cf(CF_ACCOUNT_STATES))};
  std::string start = prefix + std::string(8, '\xff');
  for (it->SeekForPrev(to_slice(start)); it->Valid() && it->key().size() == prefix.size() + 8 &&
                                          std::memcmp(it->key().data(), prefix.data(), prefix.size()) == 0 &&
                                          res.size() < limit;
       it->Prev()) {
    auto rec = read_account_state_record(td::Slice{it->value().data(), it->value().size()});
    rec.account = account;  // the key carries the identity; the value does not
    res.push_back(std::move(rec));
  }
  return res;
}

}  // namespace tonlibrarian
