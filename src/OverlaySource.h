/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <deque>
#include <map>
#include <vector>

#include "Config.h"
#include "adnl/adnl-network-manager.h"
#include "adnl/adnl-node.h"
#include "dht/dht.h"
#include "keys/keys.hpp"
#include "overlay/overlays.h"
#include "quic/quic-sender.h"
#include "rldp2/rldp.h"
#include "td/actor/actor.h"
#include "td/utils/buffer.h"
#include "ton/ton-shard.h"
#include "ton/ton-types.h"

namespace tonlibrarian {

// Block source backed by validator overlays (public shard overlays and/or
// private custom overlays). Blocks are fetched with tonNode_downloadBlockFull
// / tonNode_downloadNextBlockFull queries; broadcast data received on the
// overlays is used as a cache. Shard states are NOT available on overlays.
class OverlaySource : public td::actor::Actor {
 public:
  struct Stats {
    td::uint64 blocks_from_cache = 0;
    td::uint64 blocks_fetched = 0;
    td::uint64 fetch_failures = 0;
    td::uint64 broadcasts_received = 0;
    td::uint64 broadcasts_cached = 0;
    td::uint64 queries_sent = 0;
  };

  // Reads the global config (adnl static nodes, dht, validator zero state /
  // init block); prepares a stable ADNL identity under net_db.
  static td::Result<td::actor::ActorOwn<OverlaySource>> create(const Config::OverlayConf& conf);

  // Fetch raw block data (BoC) by exact id. Broadcast cache is consulted first.
  void get_block(const ton::BlockIdExt& id, td::Promise<td::BufferSlice> promise);

  // Fetch the masterchain block following `prev`. Resolves with the next
  // block's id (its data lands in the broadcast cache).
  void get_next_mc_block(const ton::BlockIdExt& prev, td::Promise<ton::BlockIdExt> promise);

  // Maintain the set of public shard overlays (from the current masterchain
  // shard configuration).
  void update_shards(std::vector<ton::ShardIdFull> shards);

  // The masterchain block the global config points at (zero state or
  // init_block) — the anchor for fresh overlay-only starts and overlay ids.
  void get_init_block(td::Promise<ton::BlockIdExt> promise) {
    promise.set_value(ton::BlockIdExt{init_block_});
  }

  void get_stats(td::Promise<Stats> promise) {
    promise.set_value(Stats{stats_});
  }

  Stats stats() const {
    return stats_;
  }

  // called by OverlayCallback (received block broadcasts)
  void on_broadcast(ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data);

  explicit OverlaySource(Config::OverlayConf conf, ton::Bits256 zero_state_file_hash, ton::BlockIdExt init_block,
                         ton::adnl::AdnlNodesList static_nodes,
                         std::shared_ptr<ton::dht::DhtGlobalConfig> dht_config,
                         ton::privkeys::Ed25519 private_key);

 private:
  struct CustomOverlay {
    std::string name;
    std::vector<ton::adnl::AdnlNodeIdShort> nodes;
    std::vector<ton::ShardIdFull> sender_shards;  // empty = all shards
    ton::overlay::OverlayIdFull id_full;
    ton::overlay::OverlayIdShort id_short;
    bool use_quic = false;

    bool serves_shard(const ton::ShardIdFull& shard) const {
      return sender_shards.empty() ||
             std::any_of(sender_shards.begin(), sender_shards.end(),
                         [&](const ton::ShardIdFull& s) { return ton::shard_intersects(shard, s); });
    }
  };

  struct PeerList {
    std::vector<ton::adnl::AdnlNodeIdShort> peers;
    td::Timestamp refreshed_at{};
    bool querying = false;
  };

  struct Fetch {
    td::uint64 id = 0;
    bool next_block = false;  // true: tonNode_downloadNextBlockFull(prev)
    ton::BlockIdExt block_id;
    td::Promise<td::BufferSlice> block_promise;  // get_block
    td::Promise<ton::BlockIdExt> next_promise;   // get_next_mc_block
    td::uint32 attempts = 0;
    td::uint32 in_flight = 0;
    td::uint32 peer_offset = 0;
    td::Timestamp retry_at{};
    std::string last_error;
  };

  void start_up() override;
  void tear_down() override;
  void alarm() override;

  // network stack bootstrap
  void on_key_added(td::Result<td::Unit> R);
  void create_mc_overlay();
  void create_custom_overlays();
  ton::overlay::OverlayIdShort ensure_shard_overlay(const ton::ShardIdFull& shard);
  td::actor::ActorId<ton::adnl::AdnlSenderEx> sender_ex(bool use_quic);
  td::actor::ActorId<ton::adnl::AdnlSenderInterface> sender_for(ton::overlay::OverlayIdShort overlay_id);

  // fetch machinery
  void start_fetch(Fetch fetch);
  void pump_fetch(td::uint64 fetch_id);
  void on_query_answer(td::uint64 fetch_id, ton::adnl::AdnlNodeIdShort peer, td::Result<td::BufferSlice> R);
  void fail_fetch(td::uint64 fetch_id, td::Status error);
  void finish_fetch_block(td::uint64 fetch_id, ton::BlockIdExt id, td::BufferSlice data);
  void finish_fetch_next(td::uint64 fetch_id, ton::BlockIdExt id, td::BufferSlice data);

  // peers
  void request_peers(ton::overlay::OverlayIdShort overlay_id);
  void on_peers(ton::overlay::OverlayIdShort overlay_id,
                td::Result<std::vector<ton::adnl::AdnlNodeIdShort>> peers);
  std::vector<ton::overlay::OverlayIdShort> candidate_overlays(const ton::BlockIdExt& id);

  // broadcast cache
  void cache_block(const ton::BlockIdExt& id, td::BufferSlice data);
  const td::BufferSlice* lookup_cache(const ton::BlockIdExt& id);

  Config::OverlayConf conf_;
  ton::Bits256 zero_state_file_hash_;
  ton::BlockIdExt init_block_{};
  ton::adnl::AdnlNodesList static_nodes_;
  std::shared_ptr<ton::dht::DhtGlobalConfig> dht_config_;

  // network stack
  td::actor::ActorOwn<ton::keyring::Keyring> keyring_;
  td::actor::ActorOwn<ton::adnl::AdnlNetworkManager> network_manager_;
  td::actor::ActorOwn<ton::adnl::Adnl> adnl_;
  td::actor::ActorOwn<ton::dht::Dht> dht_;
  td::actor::ActorOwn<ton::rldp2::Rldp> rldp2_;
  td::actor::ActorOwn<ton::quic::QuicSender> quic_;
  td::actor::ActorOwn<ton::overlay::Overlays> overlays_;
  ton::adnl::AdnlNodeIdFull local_id_full_{};
  ton::adnl::AdnlNodeIdShort local_id_{};
  ton::privkeys::Ed25519 private_key_;

  // overlays
  ton::overlay::OverlayIdFull mc_overlay_full_{};
  ton::overlay::OverlayIdShort mc_overlay_short_{};
  std::map<ton::ShardIdFull, ton::overlay::OverlayIdShort> shard_overlays_;
  std::vector<CustomOverlay> custom_overlays_;
  std::map<ton::overlay::OverlayIdShort, PeerList> peers_;
  td::Timestamp next_peer_refresh_{};

  // fetches
  std::map<td::uint64, Fetch> fetches_;
  td::uint64 next_fetch_id_ = 1;

  // broadcast cache (bounded)
  std::map<ton::BlockIdExt, td::BufferSlice> block_cache_;
  std::deque<ton::BlockIdExt> cache_order_;
  std::map<td::uint32, ton::BlockIdExt> mc_cache_by_seqno_;
  td::uint64 cache_bytes_ = 0;
  static constexpr size_t kMaxCachedBlocks = 512;

  Stats stats_;
};

}  // namespace tonlibrarian
