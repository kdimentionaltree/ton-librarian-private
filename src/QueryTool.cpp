/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "Storage.h"
#include "common.h"
#include "vm/vm.h"

namespace tonlibrarian {

namespace {

std::string coins_str(const Coins& c) {
  return c.grams.is_null() ? "0" : c.grams->to_dec_string();
}

std::string status_str(AccountStatus s) {
  switch (s) {
    case AccountStatus::Uninit:
      return "\"uninit\"";
    case AccountStatus::Frozen:
      return "\"frozen\"";
    case AccountStatus::Active:
      return "\"active\"";
    case AccountStatus::Nonexist:
      return "\"nonexist\"";
    default:
      return "\"?\"";
  }
}

std::string kind_str(MessageKind k) {
  switch (k) {
    case MessageKind::Int:
      return "\"int\"";
    case MessageKind::ExtIn:
      return "\"ext_in\"";
    case MessageKind::ExtOut:
      return "\"ext_out\"";
    default:
      return "\"?\"";
  }
}

// The id/hash/address helpers return JSON-ready values: quoted strings.
std::string block_id_str(const ton::BlockId& id) {
  return PSTRING() << "\"" << id.workchain << ":" << id.shard << ":" << id.seqno << "\"";
}

std::string block_id_ext_str(const ton::BlockIdExt& id) {
  return PSTRING() << "\"" << id.id.workchain << ":" << id.id.shard << ":" << id.id.seqno << ":"
                   << id.root_hash.to_hex() << ":" << id.file_hash.to_hex() << "\"";
}

std::string account_str(const Account& a) {
  return PSTRING() << "\"" << a.to_raw_string() << "\"";
}

std::string opt_account(const ParsedMessage& m, bool source) {
  if (source ? !m.has_source : !m.has_dest) {
    return "null";
  }
  return account_str(source ? m.source : m.dest);
}

void print_msg(const MsgRecord& m) {
  std::cout << "{";
  std::cout << "\"hash\":\"" << m.hash.to_hex() << "\"";
  std::cout << ",\"kind\":" << kind_str(m.kind);
  std::cout << ",\"source\":" << opt_account(m, true);
  std::cout << ",\"dest\":" << opt_account(m, false);
  std::cout << ",\"ihr_disabled\":" << (m.ihr_disabled ? "true" : "false");
  std::cout << ",\"bounce\":" << (m.bounce ? "true" : "false");
  std::cout << ",\"bounced\":" << (m.bounced ? "true" : "false");
  std::cout << ",\"created_lt\":" << m.created_lt;
  std::cout << ",\"created_at\":" << m.created_at;
  std::cout << ",\"value\":" << coins_str(m.value);
  std::cout << ",\"ihr_fee\":" << coins_str(m.ihr_fee);
  std::cout << ",\"fwd_fee\":" << coins_str(m.fwd_fee);
  std::cout << ",\"import_fee\":" << coins_str(m.import_fee);
  std::cout << ",\"extra_flags\":" << coins_str(m.extra_flags);
  if (m.has_opcode) {
    std::cout << ",\"opcode\":" << m.opcode;
  }
  if (m.has_body) {
    std::cout << ",\"body_hash\":\"" << m.body_hash.to_hex() << "\",\"body_bits\":" << m.body_bits;
  }
  if (m.has_init) {
    std::cout << ",\"init_state_hash\":\"" << m.init_state_hash.to_hex() << "\"";
  }
  if (m.has_norm_hash) {
    std::cout << ",\"norm_hash\":\"" << m.norm_hash.to_hex() << "\"";
  }
  std::cout << "}\n";
}

void print_tx(const TxRecord& t) {
  std::cout << "{";
  std::cout << "\"hash\":\"" << t.hash.to_hex() << "\"";
  std::cout << ",\"account\":" << account_str(t.account);
  std::cout << ",\"lt\":" << t.lt;
  std::cout << ",\"now\":" << t.now;
  std::cout << ",\"block\":" << block_id_str(t.block);
  std::cout << ",\"orig_status\":" << status_str(t.orig_status);
  std::cout << ",\"end_status\":" << status_str(t.end_status);
  std::cout << ",\"total_fees\":" << coins_str(t.total_fees);
  std::cout << ",\"prev_trans_hash\":\"" << t.prev_trans_hash.to_hex() << "\"";
  std::cout << ",\"prev_trans_lt\":" << t.prev_trans_lt;
  if (t.has_in_msg) {
    std::cout << ",\"in_msg\":\"" << t.in_msg.to_hex() << "\"";
  }
  std::cout << ",\"out_msgs\":[";
  for (size_t i = 0; i < t.out_msgs.size(); i++) {
    if (i) {
      std::cout << ",";
    }
    std::cout << "\"" << t.out_msgs[i].to_hex() << "\"";
  }
  std::cout << "]";
  std::cout << ",\"descr_type\":" << static_cast<unsigned>(t.descr_type);
  std::cout << ",\"aborted\":" << (t.aborted ? "true" : "false");
  std::cout << ",\"destroyed\":" << (t.destroyed ? "true" : "false");
  // compute phase
  const auto& p = t.phases;
  if (p.compute_skipped) {
    std::cout << ",\"compute\":{\"skipped\":true,\"reason\":" << static_cast<unsigned>(p.compute_skip_reason) << "}";
  } else {
    std::cout << ",\"compute\":{\"skipped\":false,\"success\":" << (p.compute_success ? "true" : "false")
              << ",\"gas_used\":" << p.compute_gas_used << ",\"gas_limit\":" << p.compute_gas_limit
              << ",\"gas_fees\":" << coins_str(p.compute_gas_fees) << ",\"exit_code\":" << p.compute_exit_code
              << ",\"vm_steps\":" << p.compute_vm_steps << "}";
  }
  if (p.has_action) {
    std::cout << ",\"action\":{\"success\":" << (p.action_success ? "true" : "false")
              << ",\"result_code\":" << p.action_result_code << ",\"total_actions\":" << p.action_tot_actions
              << ",\"msgs_created\":" << p.action_msgs_created
              << ",\"total_fwd_fees\":" << coins_str(p.action_total_fwd_fees)
              << ",\"total_action_fees\":" << coins_str(p.action_total_action_fees) << "}";
  }
  if (p.bounce_type != 0) {
    std::cout << ",\"bounce\":{\"type\":" << static_cast<unsigned>(p.bounce_type)
              << ",\"msg_fees\":" << coins_str(p.bounce_msg_fees) << "}";
  }
  std::cout << "}\n";
}

void print_account_state(const AccountStateRecord& s) {
  std::cout << "{";
  std::cout << "\"account\":" << account_str(s.account);
  std::cout << ",\"status\":" << status_str(s.status);
  std::cout << ",\"balance\":" << coins_str(s.balance);
  std::cout << ",\"last_trans_lt\":" << s.last_trans_lt;
  std::cout << ",\"last_trans_hash\":\"" << s.last_trans_hash.to_hex() << "\"";
  std::cout << ",\"state_hash\":\"" << s.state_hash.to_hex() << "\"";
  if (s.data_hash != td::Bits256::zero()) {
    std::cout << ",\"data_hash\":\"" << s.data_hash.to_hex() << "\"";
  }
  if (s.code_hash != td::Bits256::zero()) {
    std::cout << ",\"code_hash\":\"" << s.code_hash.to_hex() << "\"";
  }
  std::cout << ",\"timestamp\":" << s.gen_utime;
  std::cout << ",\"mc_seqno\":" << s.mc_seqno;
  std::cout << "}\n";
}

ton::BlockId parse_block_id(const std::string& s) {
  // wc:shard:seqno; shard follows ton's convention of hex notation
  // (masterchain = -1:8000000000000000:<seqno>)
  auto p1 = s.find(':');
  auto p2 = s.find(':', p1 + 1);
  if (p1 == std::string::npos || p2 == std::string::npos || p1 == 0 || p2 == p1 + 1 || p2 + 1 >= s.size()) {
    throw SerError("block id must look like wc:shard:seqno");
  }
  auto wc_r = td::to_integer_safe<td::int32>(s.substr(0, p1));
  auto shard_str = s.substr(p1 + 1, p2 - p1 - 1);
  if (shard_str.size() > 2 && shard_str[0] == '0' && (shard_str[1] == 'x' || shard_str[1] == 'X')) {
    shard_str = shard_str.substr(2);
  }
  auto shard_r = td::hex_to_integer_safe<td::uint64>(shard_str);
  auto seqno_r = td::to_integer_safe<td::uint32>(s.substr(p2 + 1));
  if (wc_r.is_error() || shard_r.is_error() || seqno_r.is_error()) {
    throw SerError("block id must look like wc:shard:seqno (shard in hex, e.g. -1:8000000000000000:123)");
  }
  return ton::BlockId{static_cast<ton::WorkchainId>(wc_r.move_as_ok()), shard_r.move_as_ok(), seqno_r.move_as_ok()};
}

td::uint32 parse_limit(const char* s, td::uint32 def) {
  if (s == nullptr) {
    return def;
  }
  return static_cast<td::uint32>(std::strtoul(s, nullptr, 10));
}

bool parse_reverse(const char* s) {
  return s != nullptr && std::strcmp(s, "desc") == 0;
}

int usage() {
  std::cerr << "usage: ton-librarian-query <db-path> <command> [args]\n"
            << "commands:\n"
            << "  status\n"
            << "  shards <mc-seqno>\n"
            << "  block <wc:shard:seqno>\n"
            << "  txs-of-block <wc:shard:seqno>\n"
            << "  tx <hash-hex>\n"
            << "  account-txs <wc:addr-hex> [limit=20] [asc|desc]\n"
            << "  msg <hash-hex>\n"
            << "  msg-content <hash-hex>\n"
            << "  msg-txs <hash-hex>\n"
            << "  account-msgs <wc:addr-hex> [limit=20] [asc|desc]\n"
            << "  account <wc:addr-hex>\n"
            << "  account-history <wc:addr-hex> [limit=20]\n";
  return 2;
}

int run(int argc, char** argv) {
  if (argc < 3) {
    return usage();
  }
  std::string db_path = argv[1];
  std::string cmd = argv[2];

  Storage storage{db_path, true};
  auto st = storage.open();
  if (st.is_error()) {
    std::cerr << "failed to open db: " << st.to_string() << std::endl;
    return 1;
  }

  if (cmd == "status") {
    auto p = storage.get_progress();
    std::cout << "{";
    std::cout << "\"mc_seqno\":" << p.mc_seqno;
    std::cout << ",\"mc_root_hash\":\"" << p.mc_root_hash.to_hex() << "\"";
    std::cout << ",\"mc_file_hash\":\"" << p.mc_file_hash.to_hex() << "\"";
    std::cout << ",\"mc_utime\":" << p.mc_utime;
    std::cout << ",\"total_blocks\":" << p.total_blocks;
    std::cout << ",\"total_txs\":" << p.total_txs;
    std::cout << ",\"total_msgs\":" << p.total_msgs;
    std::cout << ",\"total_account_states\":" << p.total_account_states;
    std::cout << ",\"total_slices\":" << p.total_slices;
    std::cout << "}\n";
    return 0;
  }

  if (cmd == "shards") {
    if (argc < 4) {
      return usage();
    }
    td::uint32 mc_seqno = parse_limit(argv[3], 0);
    auto tops = storage.load_prev_tops(mc_seqno);
    if (tops.empty()) {
      std::cerr << "no shards stored for mc seqno " << mc_seqno << "\n";
      return 1;
    }
    std::sort(tops.begin(), tops.end(), [](const ton::BlockIdExt& a, const ton::BlockIdExt& b) {
      return a.id.workchain < b.id.workchain || (a.id.workchain == b.id.workchain && a.id.shard < b.id.shard);
    });
    std::cout << "[";
    for (size_t i = 0; i < tops.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << block_id_ext_str(tops[i]);
    }
    std::cout << "]\n";
    return 0;
  }

  if (cmd == "block") {
    if (argc < 4) {
      return usage();
    }
    auto id = parse_block_id(argv[3]);
    auto b = storage.get_block(id);
    if (!b) {
      std::cerr << "block not found\n";
      return 1;
    }
    std::cout << "{";
    std::cout << "\"id\":" << block_id_ext_str(b->id);
    std::cout << ",\"root_hash\":\"" << b->id.root_hash.to_hex() << "\"";
    std::cout << ",\"file_hash\":\"" << b->id.file_hash.to_hex() << "\"";
    std::cout << ",\"global_id\":" << b->global_id;
    std::cout << ",\"version\":" << b->version;
    std::cout << ",\"gen_utime\":" << b->gen_utime;
    std::cout << ",\"start_lt\":" << b->start_lt;
    std::cout << ",\"end_lt\":" << b->end_lt;
    std::cout << ",\"key_block\":" << (b->key_block ? "true" : "false");
    std::cout << ",\"after_merge\":" << (b->after_merge ? "true" : "false");
    std::cout << ",\"before_split\":" << (b->before_split ? "true" : "false");
    std::cout << ",\"after_split\":" << (b->after_split ? "true" : "false");
    std::cout << ",\"gen_validator_list_hash_short\":" << b->gen_validator_list_hash_short;
    std::cout << ",\"gen_catchain_seqno\":" << b->gen_catchain_seqno;
    std::cout << ",\"min_ref_mc_seqno\":" << b->min_ref_mc_seqno;
    std::cout << ",\"prev_key_block_seqno\":" << b->prev_key_block_seqno;
    std::cout << ",\"vert_seqno\":" << b->vert_seqno;
    std::cout << ",\"software_version\":" << b->software_version;
    std::cout << ",\"software_capabilities\":" << b->software_capabilities;
    std::cout << ",\"mc_seqno\":" << b->mc_seqno;
    std::cout << ",\"prev\":[";
    for (size_t i = 0; i < b->prev.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << block_id_ext_str(b->prev[i]);
    }
    std::cout << "]";
    if (b->has_mc_blkid) {
      std::cout << ",\"mc_blkid\":" << block_id_ext_str(b->mc_blkid);
    }
    std::cout << ",\"tx_count\":" << b->tx_count;
    std::cout << "}\n";
    return 0;
  }

  if (cmd == "txs-of-block") {
    if (argc < 4) {
      return usage();
    }
    auto id = parse_block_id(argv[3]);
    auto txs = storage.get_block_txs(id);
    std::cout << "[";
    for (size_t i = 0; i < txs.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << "\"" << txs[i].to_hex() << "\"";
    }
    std::cout << "]\n";
    return 0;
  }

  if (cmd == "tx") {
    if (argc < 4) {
      return usage();
    }
    auto t = storage.get_tx(parse_hash_hex(argv[3]));
    if (!t) {
      std::cerr << "transaction not found\n";
      return 1;
    }
    print_tx(*t);
    return 0;
  }

  if (cmd == "account-txs") {
    if (argc < 4) {
      return usage();
    }
    auto account = parse_account(argv[3]);
    auto txs = storage.get_account_txs(account, parse_limit(argc > 4 ? argv[4] : nullptr, 20),
                                       parse_reverse(argc > 5 ? argv[5] : "desc"));
    std::cout << "[";
    for (size_t i = 0; i < txs.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << "\"" << txs[i].to_hex() << "\"";
    }
    std::cout << "]\n";
    return 0;
  }

  if (cmd == "msg") {
    if (argc < 4) {
      return usage();
    }
    auto m = storage.get_msg(parse_hash_hex(argv[3]));
    if (!m) {
      std::cerr << "message not found\n";
      return 1;
    }
    print_msg(*m);
    return 0;
  }

  if (cmd == "msg-content") {
    if (argc < 4) {
      return usage();
    }
    auto c = storage.get_msg_content(parse_hash_hex(argv[3]));
    if (!c) {
      std::cerr << "message content not found\n";
      return 1;
    }
    std::cout << "\"" << td::hex_encode(c->as_slice()) << "\"\n";
    return 0;
  }

  if (cmd == "msg-txs") {
    if (argc < 4) {
      return usage();
    }
    auto links = storage.get_msg_txs(parse_hash_hex(argv[3]));
    std::cout << "[";
    for (size_t i = 0; i < links.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << "{\"tx\":\"" << links[i].tx_hash.to_hex() << "\",\"dir\":"
                << static_cast<unsigned>(links[i].dir) << ",\"idx\":" << links[i].idx << "}";
    }
    std::cout << "]\n";
    return 0;
  }

  if (cmd == "account-msgs") {
    if (argc < 4) {
      return usage();
    }
    auto account = parse_account(argv[3]);
    auto msgs = storage.get_account_msgs(account, parse_limit(argc > 4 ? argv[4] : nullptr, 20),
                                         parse_reverse(argc > 5 ? argv[5] : "desc"));
    std::cout << "[";
    for (size_t i = 0; i < msgs.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << "{\"msg\":\"" << msgs[i].msg_hash.to_hex() << "\",\"dir\":"
                << static_cast<unsigned>(msgs[i].dir) << ",\"created_lt\":" << msgs[i].created_lt << "}";
    }
    std::cout << "]\n";
    return 0;
  }

  if (cmd == "account") {
    if (argc < 4) {
      return usage();
    }
    auto s = storage.get_account_state(parse_account(argv[3]));
    if (!s) {
      std::cerr << "account state not found\n";
      return 1;
    }
    print_account_state(*s);
    return 0;
  }

  if (cmd == "account-history") {
    if (argc < 4) {
      return usage();
    }
    auto hist = storage.get_account_state_history(parse_account(argv[3]),
                                                  parse_limit(argc > 4 ? argv[4] : nullptr, 20));
    std::cout << "[";
    for (size_t i = 0; i < hist.size(); i++) {
      if (i) {
        std::cout << ",";
      }
      std::cout << "\n";
      print_account_state(hist[i]);
    }
    std::cout << "]\n";
    return 0;
  }

  std::cerr << "unknown command: " << cmd << "\n";
  return usage();
}

}  // namespace

}  // namespace tonlibrarian

int main(int argc, char** argv) {
  vm::init_vm().ensure();
  try {
    return tonlibrarian::run(argc, argv);
  } catch (const tonlibrarian::SerError& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  } catch (const std::exception& err) {
    std::cerr << "error: " << err.what() << std::endl;
    return 1;
  }
}
