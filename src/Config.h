/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <string>
#include <vector>

#include "auto/tl/ton_api.h"
#include "td/utils/Slice.h"
#include "td/utils/Status.h"
#include "ton/ton-shard.h"

namespace tonlibrarian {

struct Config {
  struct Pipeline {
    td::uint32 depth = 4;
    td::uint32 block_fetch_parallelism = 16;
    td::uint32 state_fetch_parallelism = 8;
    td::uint32 parse_workers = 8;
  };

  struct AccountStatesConf {
    bool enabled = true;
    td::uint64 max_data_boc_size = 65536;
  };

  enum class SyncMode { Liteservers, Overlay, Hybrid };

  // Plain-copyable custom overlay definition (same JSON format as
  // engine.validator.customOverlay in the validator-engine config).
  struct CustomOverlayNodeConf {
    td::Bits256 adnl_id;
    bool msg_sender = false;
    td::int32 msg_sender_priority = 0;
    bool block_sender = false;
    bool accept_queries = false;
  };

  struct CustomOverlayConf {
    std::string name;
    std::vector<CustomOverlayNodeConf> nodes;
    std::vector<ton::ShardIdFull> sender_shards;  // empty = all shards
    bool use_quic = false;
  };

  struct OverlayConf {
    std::string global_config_path;
    std::string listen_addr = "0.0.0.0:30303";
    std::string net_db = "ton-db";
    bool use_public_overlays = true;
    bool use_quic = false;
    bool use_plumtree = false;
    std::vector<CustomOverlayConf> custom_overlays;
  };

  struct LiteserverConf {
    std::string global_config_path;
  };

  std::string db_path = "librarian-db";
  td::uint32 from_seqno = 0;   // first masterchain seqno to index (0 = continue existing db)
  td::uint32 until_seqno = 0;  // stop after this seqno (0 = no limit)

  Pipeline pipeline;
  AccountStatesConf account_states;

  SyncMode mode = SyncMode::Liteservers;
  LiteserverConf liteservers;
  OverlayConf overlay;

  static td::Result<Config> parse_file(const std::string& path);
  static td::Result<Config> parse(td::MutableSlice json_data);
  td::Status validate() const;
};

}  // namespace tonlibrarian
