/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "BlockParser.h"

#include <algorithm>

#include "block/block-auto.h"
#include "block/mc-config.h"
#include "block/block-parse.h"
#include "common/checksum.h"
#include "crypto/common/refint.h"
#include "crypto/vm/boc.h"
#include "crypto/vm/cells/CellBuilder.h"
#include "tl/tlblib.hpp"
#include "ton/ton-shard.h"

namespace tonlibrarian {

using namespace block::gen;

namespace {

Coins parse_coins(td::Ref<vm::CellSlice> csr) {
  Coins c;
  td::RefInt256 grams;
  td::Ref<vm::Cell> extra;
  if (!block::unpack_CurrencyCollection(csr, grams, extra)) {
    throw SerError("failed to unpack CurrencyCollection");
  }
  c.grams = std::move(grams);
  c.extra = std::move(extra);
  return c;
}

Coins parse_grams(td::Ref<vm::CellSlice> csr) {
  auto g = block::tlb::t_Grams.as_integer_skip(csr.write());
  Coins c;
  c.grams = std::move(g);
  return c;
}

AccountStatus parse_account_status(char status) {
  switch (status) {
    case 0:
      return AccountStatus::Uninit;
    case 1:
      return AccountStatus::Frozen;
    case 2:
      return AccountStatus::Active;
    case 3:
      return AccountStatus::Nonexist;
    default:
      throw SerError("bad account status");
  }
}

// Extracts (wc, addr) from a MsgAddressInt slice.
bool extract_address(td::Ref<vm::CellSlice> csr, Account& out) {
  block::StdAddress addr;
  if (!block::tlb::t_MsgAddressInt.extract_std_address(std::move(csr), addr)) {
    return false;
  }
  out = Account{static_cast<td::int8>(addr.workchain), addr.addr};
  return true;
}

// Normalized hash of an external-in message (src replaced with addr_none,
// import_fee zeroed). Mirrors ton-indexer's ext_in_msg_get_normalized_hash.
td::Result<td::Bits256> ext_in_msg_normalized_hash(const td::Ref<vm::Cell>& msg_cell) {
  block::gen::Message::Record message;
  if (!tlb::type_unpack_cell(msg_cell, block::gen::t_Message_Any, message)) {
    return td::Status::Error("failed to unpack Message");
  }
  auto tag = block::gen::t_CommonMsgInfo.get_tag(*message.info);
  if (tag != block::gen::CommonMsgInfo::ext_in_msg_info) {
    return td::Status::Error("not an ext_in message");
  }
  block::gen::CommonMsgInfo::Record_ext_in_msg_info msg_info;
  if (!tlb::csr_unpack(message.info, msg_info)) {
    return td::Status::Error("failed to unpack ext_in_msg_info");
  }
  td::Ref<vm::Cell> body;
  auto body_cs = message.body.write();
  if (body_cs.fetch_ulong(1) == 1) {
    body = body_cs.fetch_ref();
  } else {
    body = vm::CellBuilder().append_cellslice(body_cs).finalize();
  }
  auto cb = vm::CellBuilder();
  bool ok = cb.store_long_bool(2, 2) &&              // ext_in_msg_info$10
            cb.store_long_bool(0, 2) &&              // src:MsgAddressExt -> addr_none$00
            cb.append_cellslice_bool(msg_info.dest)  // dest:MsgAddressInt
            && cb.store_long_bool(0, 4)             // import_fee:Grams -> 0
            && cb.store_long_bool(0, 1)              // init:(Maybe ...) -> nothing$0
            && cb.store_long_bool(1, 1)              // body:(Either X ^X) -> right$1
            && cb.store_ref_bool(body);
  if (!ok) {
    return td::Status::Error("failed to build normalized message");
  }
  return cb.finalize()->get_hash().bits();
}

td::Result<ParsedMessage> parse_message(const td::Ref<vm::Cell>& msg_cell) {
  ParsedMessage msg;
  msg.hash = msg_cell->get_hash().bits();

  block::gen::Message::Record message;
  if (!tlb::type_unpack_cell(msg_cell, block::gen::t_Message_Any, message)) {
    return td::Status::Error("failed to unpack Message");
  }

  // body: Either X ^X
  {
    td::Ref<vm::Cell> body_cell;
    auto& body_cs = message.body.write();
    if (body_cs.fetch_ulong(1) == 0) {
      body_cell = vm::CellBuilder().append_cellslice(body_cs).finalize();
    } else {
      body_cell = body_cs.fetch_ref();
    }
    vm::CellSlice body{vm::NoVmOrd(), body_cell};
    if (body.size() > 0 || body.size_refs() > 0) {
      msg.has_body = true;
      msg.body_hash = body_cell->get_hash().bits();
      msg.body_bits = body.size();
      auto r = vm::std_boc_serialize(body_cell, 31);
      if (r.is_error()) {
        return td::Status::Error(PSLICE() << "failed to serialize message body: " << r.error());
      }
      msg.body_boc = r.move_as_ok();
      if (body.size() >= 32) {
        msg.has_opcode = true;
        msg.opcode = static_cast<td::int32>(body.prefetch_long(32));
      }
    }
  }

  // init: Maybe (Either StateInit ^StateInit)
  {
    auto& init_cs = message.init.write();
    if (init_cs.fetch_ulong(1) == 1) {
      td::Ref<vm::Cell> init_cell;
      if (init_cs.fetch_ulong(1) == 0) {
        init_cell = vm::CellBuilder().append_cellslice(init_cs).finalize();
      } else {
        init_cell = init_cs.fetch_ref();
      }
      if (init_cell.is_null()) {
        return td::Status::Error("null init cell");
      }
      msg.has_init = true;
      msg.init_state_hash = init_cell->get_hash().bits();
    }
  }

  auto tag = block::gen::t_CommonMsgInfo.get_tag(*message.info);
  switch (tag) {
    case block::gen::CommonMsgInfo::int_msg_info: {
      block::gen::CommonMsgInfo::Record_int_msg_info msg_info;
      if (!tlb::csr_unpack(message.info, msg_info)) {
        return td::Status::Error("failed to unpack int_msg_info");
      }
      msg.kind = MessageKind::Int;
      msg.ihr_disabled = msg_info.ihr_disabled;
      msg.bounce = msg_info.bounce;
      msg.bounced = msg_info.bounced;
      msg.value = parse_coins(msg_info.value);
      // Note: since global version 12 the field at the ihr_fee position is
      // extra_flags; ihr_fee is effectively zero on modern networks.
      msg.extra_flags = parse_grams(msg_info.extra_flags);
      msg.fwd_fee = parse_grams(msg_info.fwd_fee);
      msg.created_lt = msg_info.created_lt;
      msg.created_at = msg_info.created_at;
      msg.has_source = true;
      if (!extract_address(msg_info.src, msg.source)) {
        return td::Status::Error("failed to parse message source");
      }
      msg.has_dest = true;
      if (!extract_address(msg_info.dest, msg.dest)) {
        return td::Status::Error("failed to parse message destination");
      }
      break;
    }
    case block::gen::CommonMsgInfo::ext_in_msg_info: {
      block::gen::CommonMsgInfo::Record_ext_in_msg_info msg_info;
      if (!tlb::csr_unpack(message.info, msg_info)) {
        return td::Status::Error("failed to unpack ext_in_msg_info");
      }
      msg.kind = MessageKind::ExtIn;
      msg.has_dest = true;
      if (!extract_address(msg_info.dest, msg.dest)) {
        return td::Status::Error("failed to parse message destination");
      }
      msg.import_fee = parse_grams(msg_info.import_fee);
      auto norm = ext_in_msg_normalized_hash(msg_cell);
      if (norm.is_ok()) {
        msg.has_norm_hash = true;
        msg.norm_hash = norm.move_as_ok();
      }
      break;
    }
    case block::gen::CommonMsgInfo::ext_out_msg_info: {
      block::gen::CommonMsgInfo::Record_ext_out_msg_info msg_info;
      if (!tlb::csr_unpack(message.info, msg_info)) {
        return td::Status::Error("failed to unpack ext_out_msg_info");
      }
      msg.kind = MessageKind::ExtOut;
      msg.has_source = true;
      if (!extract_address(msg_info.src, msg.source)) {
        return td::Status::Error("failed to parse message source");
      }
      msg.created_lt = msg_info.created_lt;
      msg.created_at = msg_info.created_at;
      break;
    }
    default:
      return td::Status::Error("unknown CommonMsgInfo tag");
  }
  return msg;
}

td::uint64 varuint7_as_uint(const vm::CellSlice& cs) {
  return block::tlb::t_VarUInteger_7.as_uint(cs);
}

// StorageUsed: cells:(VarUInteger 7) bits:(VarUInteger 7)
void parse_storage_used(td::Ref<vm::CellSlice> csr, td::uint32& cells, td::uint32& bits) {
  StorageUsed::Record used;
  if (!tlb::csr_unpack(std::move(csr), used)) {
    throw SerError("failed to unpack StorageUsed");
  }
  cells = static_cast<td::uint32>(varuint7_as_uint(*used.cells));
  bits = static_cast<td::uint32>(varuint7_as_uint(*used.bits));
}

void parse_storage_phase(vm::CellSlice& cs, ParsedPhases& out) {
  TrStoragePhase::Record ph;
  if (!tlb::unpack(cs, ph)) {
    throw SerError("failed to unpack TrStoragePhase");
  }
  out.has_storage = true;
  out.storage_fees_collected = parse_grams(ph.storage_fees_collected);
  auto& due = ph.storage_fees_due.write();
  if (due.fetch_ulong(1) == 1) {
    out.storage_fees_due = Coins{block::tlb::t_Grams.as_integer_skip(due)};
  }
  out.storage_status_change = static_cast<td::int8>(ph.status_change);
}

void parse_credit_phase(vm::CellSlice& cs, ParsedPhases& out) {
  TrCreditPhase::Record ph;
  if (!tlb::unpack(cs, ph)) {
    throw SerError("failed to unpack TrCreditPhase");
  }
  out.has_credit = true;
  auto& due = ph.due_fees_collected.write();
  if (due.fetch_ulong(1) == 1) {
    out.credit_due_fees = Coins{block::tlb::t_Grams.as_integer_skip(due)};
  }
  out.credit = parse_coins(ph.credit);
}

void parse_compute_phase(vm::CellSlice& cs, ParsedPhases& out) {
  int tag = block::gen::t_TrComputePhase.get_tag(cs);
  if (tag == TrComputePhase::tr_phase_compute_vm) {
    TrComputePhase::Record_tr_phase_compute_vm ph;
    if (!tlb::unpack(cs, ph)) {
      throw SerError("failed to unpack tr_phase_compute_vm");
    }
    out.compute_skipped = false;
    out.compute_success = ph.success;
    out.compute_msg_state_used = ph.msg_state_used;
    out.compute_account_activated = ph.account_activated;
    out.compute_gas_fees = parse_grams(ph.gas_fees);
    out.compute_gas_used = varuint7_as_uint(*ph.r1.gas_used);
    out.compute_gas_limit = varuint7_as_uint(*ph.r1.gas_limit);
    auto& gas_credit = ph.r1.gas_credit.write();
    if (gas_credit.fetch_ulong(1) == 1) {
      out.compute_gas_credit = static_cast<td::int64>(block::tlb::t_VarUInteger_3.as_uint(gas_credit));
    }
    out.compute_mode = static_cast<td::int8>(ph.r1.mode);
    out.compute_exit_code = ph.r1.exit_code;
    auto& exit_arg = ph.r1.exit_arg.write();
    if (exit_arg.fetch_ulong(1) == 1) {
      out.compute_has_exit_arg = true;
      out.compute_exit_arg = exit_arg.fetch_long(32);
    }
    out.compute_vm_steps = ph.r1.vm_steps;
    out.compute_vm_init_state_hash = ph.r1.vm_init_state_hash;
    out.compute_vm_final_state_hash = ph.r1.vm_final_state_hash;
  } else if (tag == TrComputePhase::tr_phase_compute_skipped) {
    TrComputePhase::Record_tr_phase_compute_skipped ph;
    if (!tlb::unpack(cs, ph)) {
      throw SerError("failed to unpack tr_phase_compute_skipped");
    }
    out.compute_skipped = true;
    out.compute_skip_reason = static_cast<td::uint8>(ph.reason);
  } else {
    throw SerError("unknown TrComputePhase tag");
  }
}

void parse_action_phase(vm::CellSlice& cs, ParsedPhases& out) {
  TrActionPhase::Record ph;
  if (!tlb::unpack(cs, ph)) {
    throw SerError("failed to unpack TrActionPhase");
  }
  out.has_action = true;
  out.action_success = ph.success;
  out.action_valid = ph.valid;
  out.action_no_funds = ph.no_funds;
  out.action_status_change = static_cast<td::int8>(ph.status_change);
  auto& fwd = ph.total_fwd_fees.write();
  if (fwd.fetch_ulong(1) == 1) {
    out.action_total_fwd_fees = Coins{block::tlb::t_Grams.as_integer_skip(fwd)};
  }
  auto& act = ph.total_action_fees.write();
  if (act.fetch_ulong(1) == 1) {
    out.action_total_action_fees = Coins{block::tlb::t_Grams.as_integer_skip(act)};
  }
  out.action_result_code = ph.result_code;
  auto& res_arg = ph.result_arg.write();
  if (res_arg.fetch_ulong(1) == 1) {
    out.action_has_result_arg = true;
    out.action_result_arg = res_arg.fetch_long(32);
  }
  out.action_tot_actions = static_cast<td::uint16>(ph.tot_actions);
  out.action_spec_actions = static_cast<td::uint16>(ph.spec_actions);
  out.action_skipped_actions = static_cast<td::uint16>(ph.skipped_actions);
  out.action_msgs_created = static_cast<td::uint16>(ph.msgs_created);
  out.action_list_hash = ph.action_list_hash;
  // tot_msg_size: StorageUsed
  parse_storage_used(ph.tot_msg_size, out.action_tot_msg_cells, out.action_tot_msg_bits);
}

void parse_bounce_phase(vm::CellSlice& cs, ParsedPhases& out) {
  int tag = block::gen::t_TrBouncePhase.get_tag(cs);
  switch (tag) {
    case TrBouncePhase::tr_phase_bounce_negfunds: {
      TrBouncePhase::Record_tr_phase_bounce_negfunds ph;
      if (!tlb::unpack(cs, ph)) {
        throw SerError("failed to unpack tr_phase_bounce_negfunds");
      }
      out.bounce_type = 1;
      break;
    }
    case TrBouncePhase::tr_phase_bounce_nofunds: {
      TrBouncePhase::Record_tr_phase_bounce_nofunds ph;
      if (!tlb::unpack(cs, ph)) {
        throw SerError("failed to unpack tr_phase_bounce_nofunds");
      }
      out.bounce_type = 2;
      parse_storage_used(ph.msg_size, out.bounce_msg_cells, out.bounce_msg_bits);
      out.bounce_req_fwd_fees = parse_grams(ph.req_fwd_fees);
      break;
    }
    case TrBouncePhase::tr_phase_bounce_ok: {
      TrBouncePhase::Record_tr_phase_bounce_ok ph;
      if (!tlb::unpack(cs, ph)) {
        throw SerError("failed to unpack tr_phase_bounce_ok");
      }
      out.bounce_type = 3;
      parse_storage_used(ph.msg_size, out.bounce_msg_cells, out.bounce_msg_bits);
      out.bounce_msg_fees = parse_grams(ph.msg_fees);
      out.bounce_fwd_fees = parse_grams(ph.fwd_fees);
      break;
    }
    default:
      throw SerError("unknown TrBouncePhase tag");
  }
}

void parse_split_merge_info(td::Ref<vm::CellSlice> csr, ParsedTransaction& out) {
  SplitMergeInfo::Record info;
  if (!tlb::csr_unpack(csr, info)) {
    throw SerError("failed to unpack SplitMergeInfo");
  }
  out.has_split_merge_info = true;
  out.sm_cur_shard_pfx_len = static_cast<td::uint8>(info.cur_shard_pfx_len);
  out.sm_acc_split_depth = static_cast<td::uint8>(info.acc_split_depth);
  out.sm_this_addr = info.this_addr;
  out.sm_sibling_addr = info.sibling_addr;
}

void parse_transaction_descr(vm::CellSlice& cs, ParsedTransaction& out) {
  int tag = block::gen::t_TransactionDescr.get_tag(cs);
  switch (tag) {
    case TransactionDescr::trans_ord: {
      TransactionDescr::Record_trans_ord ord;
      if (!tlb::unpack(cs, ord)) {
        throw SerError("failed to unpack trans_ord");
      }
      out.descr_type = 0;
      out.credit_first = ord.credit_first;
      auto& s = ord.storage_ph.write();
      if (s.fetch_ulong(1) == 1) {
        parse_storage_phase(s, out.phases);
      }
      auto& c = ord.credit_ph.write();
      if (c.fetch_ulong(1) == 1) {
        parse_credit_phase(c, out.phases);
      }
      parse_compute_phase(ord.compute_ph.write(), out.phases);
      auto& a = ord.action.write();
      if (a.fetch_ulong(1) == 1) {
        auto action_cs = vm::load_cell_slice(a.fetch_ref());
        parse_action_phase(action_cs, out.phases);
      }
      out.aborted = ord.aborted;
      auto& b = ord.bounce.write();
      if (b.fetch_ulong(1) == 1) {
        parse_bounce_phase(b, out.phases);
      }
      out.destroyed = ord.destroyed;
      break;
    }
    case TransactionDescr::trans_storage: {
      TransactionDescr::Record_trans_storage st;
      if (!tlb::unpack(cs, st)) {
        throw SerError("failed to unpack trans_storage");
      }
      out.descr_type = 1;
      parse_storage_phase(st.storage_ph.write(), out.phases);
      break;
    }
    case TransactionDescr::trans_tick_tock: {
      TransactionDescr::Record_trans_tick_tock tt;
      if (!tlb::unpack(cs, tt)) {
        throw SerError("failed to unpack trans_tick_tock");
      }
      out.descr_type = 2;
      out.is_tock = tt.is_tock;
      parse_storage_phase(tt.storage_ph.write(), out.phases);
      parse_compute_phase(tt.compute_ph.write(), out.phases);
      auto& a = tt.action.write();
      if (a.fetch_ulong(1) == 1) {
        auto action_cs = vm::load_cell_slice(a.fetch_ref());
        parse_action_phase(action_cs, out.phases);
      }
      out.aborted = tt.aborted;
      out.destroyed = tt.destroyed;
      break;
    }
    case TransactionDescr::trans_split_prepare: {
      TransactionDescr::Record_trans_split_prepare sp;
      if (!tlb::unpack(cs, sp)) {
        throw SerError("failed to unpack trans_split_prepare");
      }
      out.descr_type = 3;
      parse_split_merge_info(sp.split_info, out);
      auto& s = sp.storage_ph.write();
      if (s.fetch_ulong(1) == 1) {
        parse_storage_phase(s, out.phases);
      }
      parse_compute_phase(sp.compute_ph.write(), out.phases);
      auto& a = sp.action.write();
      if (a.fetch_ulong(1) == 1) {
        auto action_cs = vm::load_cell_slice(a.fetch_ref());
        parse_action_phase(action_cs, out.phases);
      }
      out.aborted = sp.aborted;
      out.destroyed = sp.destroyed;
      break;
    }
    case TransactionDescr::trans_split_install: {
      TransactionDescr::Record_trans_split_install si;
      if (!tlb::unpack(cs, si)) {
        throw SerError("failed to unpack trans_split_install");
      }
      out.descr_type = 4;
      parse_split_merge_info(si.split_info, out);
      out.sm_prepare_tx_hash = si.prepare_transaction->get_hash().bits();
      out.sm_installed = si.installed;
      break;
    }
    case TransactionDescr::trans_merge_prepare: {
      TransactionDescr::Record_trans_merge_prepare mp;
      if (!tlb::unpack(cs, mp)) {
        throw SerError("failed to unpack trans_merge_prepare");
      }
      out.descr_type = 5;
      parse_split_merge_info(mp.split_info, out);
      parse_storage_phase(mp.storage_ph.write(), out.phases);
      out.aborted = mp.aborted;
      break;
    }
    case TransactionDescr::trans_merge_install: {
      TransactionDescr::Record_trans_merge_install mi;
      if (!tlb::unpack(cs, mi)) {
        throw SerError("failed to unpack trans_merge_install");
      }
      out.descr_type = 6;
      parse_split_merge_info(mi.split_info, out);
      out.sm_prepare_tx_hash = mi.prepare_transaction->get_hash().bits();
      auto& s = mi.storage_ph.write();
      if (s.fetch_ulong(1) == 1) {
        parse_storage_phase(s, out.phases);
      }
      auto& c = mi.credit_ph.write();
      if (c.fetch_ulong(1) == 1) {
        parse_credit_phase(c, out.phases);
      }
      parse_compute_phase(mi.compute_ph.write(), out.phases);
      auto& a = mi.action.write();
      if (a.fetch_ulong(1) == 1) {
        auto action_cs = vm::load_cell_slice(a.fetch_ref());
        parse_action_phase(action_cs, out.phases);
      }
      out.aborted = mi.aborted;
      out.destroyed = mi.destroyed;
      break;
    }
    default:
      throw SerError("unknown TransactionDescr tag");
  }
}

void parse_transactions(const ton::BlockIdExt& blk_id, const BlockExtra::Record& extra, ParsedBlock& out) {
  vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                   block::tlb::aug_ShardAccountBlocks};
  td::Bits256 cur_addr = td::Bits256::zero();
  bool eof = false;
  bool allow_same = true;
  while (!eof) {
    td::Ref<vm::CellSlice> value;
    try {
      value = acc_dict.extract_value(acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true,
                                                                                     allow_same));
    } catch (const vm::VmError& err) {
      throw SerError((PSLICE() << "error traversing account blocks: " << err.get_msg()).str());
    }
    if (value.is_null()) {
      eof = true;
      break;
    }
    allow_same = false;
    AccountBlock::Record acc_blk;
    if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
      throw SerError((PSLICE() << "invalid AccountBlock for account " << cur_addr.to_hex()).str());
    }
    vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64,
                                       block::tlb::aug_AccountTransactions};
    // NB: the explicit (long long) cast matters: {0} would resolve to the
    // raw_byte_array_t (const unsigned char*) constructor via the null pointer
    // constant and memcpy() from null.
    td::BitArray<64> cur_trans{(long long)0};
    while (true) {
      td::Ref<vm::Cell> tvalue;
      try {
        tvalue = trans_dict.extract_value_ref(trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64,
                                                                                                  true));
      } catch (const vm::VmError& err) {
        throw SerError((PSLICE() << "error traversing transactions: " << err.get_msg()).str());
      }
      if (tvalue.is_null()) {
        break;
      }
      Transaction::Record trans;
      if (!tlb::unpack_cell(tvalue, trans)) {
        throw SerError("failed to unpack Transaction");
      }
      ParsedTransaction tx;
      tx.account = Account{static_cast<td::int8>(blk_id.id.workchain), cur_addr};
      tx.hash = tvalue->get_hash().bits();
      tx.lt = trans.lt;
      tx.prev_trans_hash = trans.prev_trans_hash;
      tx.prev_trans_lt = trans.prev_trans_lt;
      tx.now = trans.now;
      tx.orig_status = parse_account_status(trans.orig_status);
      tx.end_status = parse_account_status(trans.end_status);
      tx.total_fees = parse_coins(trans.total_fees);

      if (trans.r1.in_msg.not_null() && trans.r1.in_msg->prefetch_long(1)) {
        auto msg_cell = trans.r1.in_msg->prefetch_ref();
        if (msg_cell.is_null()) {
          throw SerError("null in_msg cell");
        }
        auto msg = parse_message(msg_cell);
        if (msg.is_error()) {
          throw SerError((PSLICE() << "failed to parse in_msg of " << tx.hash.to_hex() << ": " << msg.error()).str());
        }
        ParsedMessage m = msg.move_as_ok();
        // ext_in messages carry no created_lt in their CommonMsgInfo; use the
        // importing transaction's lt/now so that msg_by_account ordering works.
        if (m.kind == MessageKind::ExtIn && m.created_lt == 0) {
          m.created_lt = trans.lt;
          m.created_at = trans.now;
        }
        tx.has_in_msg = true;
        tx.in_msg = m.hash;
        out.messages.emplace(m.hash, std::move(m));
      }
      if (trans.outmsg_cnt != 0) {
        vm::Dictionary dict{trans.r1.out_msgs, 15};
        for (int x = 0; x < trans.outmsg_cnt; x++) {
          auto msg_cell = dict.lookup_ref(td::BitArray<15>{x});
          if (msg_cell.is_null()) {
            throw SerError((PSLICE() << "missing out message " << x << " of " << tx.hash.to_hex()).str());
          }
          auto msg = parse_message(msg_cell);
          if (msg.is_error()) {
            throw SerError((PSLICE() << "failed to parse out_msg of " << tx.hash.to_hex() << ": " << msg.error()).str());
          }
          ParsedMessage m = msg.move_as_ok();
          tx.out_msgs.push_back(m.hash);
          out.messages.emplace(m.hash, std::move(m));
        }
      }

      HASH_UPDATE::Record state_hash_update;
      if (!tlb::type_unpack_cell(std::move(trans.state_update), block::gen::t_HASH_UPDATE_Account,
                                 state_hash_update)) {
        throw SerError("failed to unpack state_update");
      }
      tx.state_hash_before = state_hash_update.old_hash;
      tx.state_hash_after = state_hash_update.new_hash;

      auto descr = vm::load_cell_slice(trans.description);
      parse_transaction_descr(descr, tx);

      out.txs.push_back(std::move(tx));
    }
  }
}

ParsedAccountState parse_account_cell(const td::Ref<vm::Cell>& account_cell, td::int8 wc, td::uint32 gen_utime,
                                      size_t max_boc_size) {
  ParsedAccountState st;
  st.account.wc = wc;
  st.gen_utime = gen_utime;
  st.state_hash = account_cell->get_hash().bits();
  int tag = block::gen::t_Account.get_tag(vm::CellSlice{vm::NoVmOrd(), account_cell});
  if (tag == block::gen::Account::account_none) {
    st.status = AccountStatus::Nonexist;
    st.last_trans_lt = 0;
    return st;
  }
  if (tag != block::gen::Account::account) {
    throw SerError("unknown Account tag");
  }
  block::gen::Account::Record_account rec;
  if (!tlb::unpack_cell(account_cell, rec)) {
    throw SerError("failed to unpack Account");
  }
  // addr: MsgAddressInt
  if (!extract_address(rec.addr, st.account)) {
    throw SerError("failed to parse account address");
  }
  AccountStorage::Record storage;
  if (!tlb::csr_unpack(rec.storage, storage)) {
    throw SerError("failed to unpack AccountStorage");
  }
  st.balance = parse_coins(storage.balance);
  st.last_trans_lt = storage.last_trans_lt;

  auto& state = storage.state.write();
  int state_tag = block::gen::t_AccountState.get_tag(state);
  if (state_tag == AccountState::account_uninit) {
    st.status = AccountStatus::Uninit;
  } else if (state_tag == AccountState::account_frozen) {
    AccountState::Record_account_frozen frozen;
    if (!tlb::unpack(state, frozen)) {
      throw SerError("failed to unpack account_frozen");
    }
    st.status = AccountStatus::Frozen;
    st.frozen_hash = frozen.state_hash;
  } else if (state_tag == AccountState::account_active) {
    AccountState::Record_account_active active;
    if (!tlb::unpack(state, active)) {
      throw SerError("failed to unpack account_active");
    }
    st.status = AccountStatus::Active;
    StateInit::Record init;
    if (!tlb::csr_unpack(active.x, init)) {
      throw SerError("failed to unpack StateInit");
    }
    auto take_cell = [&](td::Ref<vm::CellSlice> maybe, td::Bits256& hash, td::BufferSlice& boc) {
      auto& cs = maybe.write();
      if (cs.fetch_ulong(1) == 1) {
        auto cell = cs.fetch_ref();
        if (cell.is_null()) {
          throw SerError("null StateInit cell");
        }
        hash = cell->get_hash().bits();
        if (max_boc_size > 0) {
          auto r = vm::std_boc_serialize(cell, 31);
          if (r.is_ok() && r.ok_ref().size() <= max_boc_size) {
            boc = r.move_as_ok();
          }
        }
      }
    };
    take_cell(init.code, st.code_hash, st.code_boc);
    take_cell(init.data, st.data_hash, st.data_boc);
  } else {
    throw SerError("unknown AccountState tag");
  }
  return st;
}

}  // namespace

td::Result<ParsedBlock> parse_block(const ton::BlockIdExt& id, td::BufferSlice data) {
  try {
    ton::FileHash computed_file_hash;
    computed_file_hash.as_slice().copy_from(td::sha256_bits256(data.as_slice()).as_slice());
    if (computed_file_hash != id.file_hash) {
      return td::Status::Error(PSLICE() << "file hash mismatch for " << id.to_str());
    }
    TRY_RESULT(root, vm::std_boc_deserialize(std::move(data)));
    if (td::Bits256{root->get_hash().bits()} != id.root_hash) {
      return td::Status::Error(PSLICE() << "root hash mismatch for " << id.to_str());
    }

    Block::Record blk;
    BlockInfo::Record info;
    BlockExtra::Record extra;
    if (!(tlb::unpack_cell(root, blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
      return td::Status::Error(PSLICE() << "cannot unpack block " << id.to_str());
    }

    ParsedBlock out;
    out.id = id;
    out.global_id = blk.global_id;
    out.version = info.version;
    out.not_master = info.not_master;
    out.after_merge = info.after_merge;
    out.before_split = info.before_split;
    out.after_split = info.after_split;
    out.want_merge = info.want_merge;
    out.want_split = info.want_split;
    out.key_block = info.key_block;
    out.vert_seqno_incr = info.vert_seqno_incr;
    out.flags = info.flags;
    out.gen_utime = info.gen_utime;
    out.start_lt = info.start_lt;
    out.end_lt = info.end_lt;
    out.gen_validator_list_hash_short = info.gen_validator_list_hash_short;
    out.gen_catchain_seqno = info.gen_catchain_seqno;
    out.min_ref_mc_seqno = info.min_ref_mc_seqno;
    out.prev_key_block_seqno = info.prev_key_block_seqno;
    out.vert_seqno = info.vert_seq_no;
    if (info.gen_software.not_null()) {
      GlobalVersion::Record ver;
      if (tlb::csr_unpack(info.gen_software, ver)) {
        out.software_version = ver.version;
        out.software_capabilities = ver.capabilities;
      }
    }

    TRY_RESULT(header, block::get_block_header_info(root, id));
    out.prev = std::move(header.prev);
    out.mc_blkid = header.mc_blkid;
    out.after_split_ = header.after_split;

    if (id.is_masterchain()) {
      // custom: Maybe ^McBlockExtra -> shard_hashes
      auto& custom = extra.custom.write();
      if (custom.fetch_ulong(1) == 1) {
        auto mc_extra_cell = custom.fetch_ref();
        McBlockExtra::Record mc_extra;
        if (!tlb::unpack_cell(std::move(mc_extra_cell), mc_extra)) {
          return td::Status::Error("cannot unpack McBlockExtra");
        }
        block::ShardConfig shard_config;
        if (!shard_config.unpack(std::move(mc_extra.shard_hashes))) {
          return td::Status::Error("cannot unpack ShardHashes");
        }
        for (const auto& blkid : shard_config.get_shard_hash_ids(true)) {
          auto shard_hash = shard_config.get_shard_hash(ton::ShardIdFull{blkid.workchain, blkid.shard}, false);
          if (shard_hash.is_null()) {
            return td::Status::Error(PSLICE() << "no shard hash for " << static_cast<td::int32>(blkid.workchain) << ":"
                                              << blkid.shard);
          }
          if (blkid.seqno == 0) {
            // brand-new shard: its first block is not produced yet
            continue;
          }
          out.tops.push_back(shard_hash->top_block_id());
        }
      } else {
        return td::Status::Error("masterchain block without McBlockExtra");
      }
    }

    parse_transactions(id, extra, out);
    return std::move(out);
  } catch (const vm::VmError& err) {
    return td::Status::Error(PSLICE() << "vm error while parsing block " << id.to_str() << ": " << err.get_msg());
  } catch (const vm::VmVirtError& err) {
    return td::Status::Error(PSLICE() << "vm virtualization error while parsing block " << id.to_str() << ": "
                                      << err.get_msg());
  } catch (const SerError& err) {
    return td::Status::Error(PSLICE() << "cannot parse block " << id.to_str() << ": " << err.what());
  }
}

td::Result<ParsedAccountState> parse_account_state(td::BufferSlice boc, const Account& account,
                                                   td::uint32 gen_utime, size_t max_boc_size) {
  try {
    if (boc.empty()) {
      // liteservers answer with an empty state for accounts that do not exist
      ParsedAccountState st;
      st.account = account;
      st.status = AccountStatus::Nonexist;
      return st;
    }
    TRY_RESULT(cell, vm::std_boc_deserialize(std::move(boc)));
    ParsedAccountState st = parse_account_cell(cell, account.wc, gen_utime, max_boc_size);
    st.account = account;  // keep the queried identity (none cells carry no address)
    return st;
  } catch (const vm::VmError& err) {
    return td::Status::Error(PSLICE() << "vm error while parsing account state of " << account.addr.to_hex() << ": "
                                      << err.get_msg());
  } catch (const SerError& err) {
    return td::Status::Error(PSLICE() << "cannot parse account state of " << account.addr.to_hex() << ": "
                                      << err.what());
  }
}

}  // namespace tonlibrarian
