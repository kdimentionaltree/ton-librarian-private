/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include "common.h"
#include "lite-client/ext-client.h"
#include "td/actor/actor.h"
#include "td/utils/buffer.h"
#include "ton/ton-types.h"

namespace tonlibrarian {

// Block/state source backed by liteservers from a global config.
// Handles failover (inside ExtClient) and retries with backoff (per query).
class LiteserverSource : public td::actor::Actor {
 public:
  static td::Result<std::vector<liteclient::LiteServerConfig>> parse_global_config(const std::string& path);
  static td::Result<td::actor::ActorOwn<LiteserverSource>> create(const std::string& global_config_path);

  explicit LiteserverSource(std::vector<liteclient::LiteServerConfig> servers);

  // Latest masterchain block id.
  void get_masterchain_info(td::Promise<ton::BlockIdExt> promise);
  // Masterchain block id by seqno.
  void lookup_mc_block(td::uint32 seqno, td::Promise<ton::BlockIdExt> promise);
  // Raw block data (BoC) for the given block id.
  void get_block(const ton::BlockIdExt& id, td::Promise<td::BufferSlice> promise);
  // Account state cell (BoC) of `account`, resolved at the given masterchain
  // block. liteservers refuse to serve full shard states for recent blocks
  // (seqno > 1000), so touched-account states are fetched one by one.
  void get_account_state(const ton::BlockIdExt& id, const Account& account, td::Promise<td::BufferSlice> promise);

 private:
  void start_up() override;

  std::vector<liteclient::LiteServerConfig> servers_;
  td::actor::ActorOwn<liteclient::ExtClient> client_;
};

}  // namespace tonlibrarian
