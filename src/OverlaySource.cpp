/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "OverlaySource.h"

#include <algorithm>
#include <set>
#include <utility>

#include "adnl/adnl-peer-table.h"
#include "auto/tl/ton_api.h"
#include "auto/tl/ton_api_json.h"
#include "common/checksum.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/Random.h"
#include "td/utils/filesystem.h"
#include "td/utils/format.h"
#include "td/utils/lz4.h"
#include "td/utils/port/path.h"
#include "tl-utils/common-utils.hpp"
#include "tl/tl_json.h"
#include "ton/ton-io.hpp"
#include "ton/ton-shard.h"
#include "ton/ton-tl.hpp"
#include "vm/boc.h"

namespace tonlibrarian {

namespace {

// tonNode_downloadBlockFull answers may contain proof (up to 4 MB) + block
// (up to 4 MB) + tl overhead.
constexpr td::uint64 kMaxAnswerSize = (4u << 20) + (4u << 20) + 128;
constexpr td::int32 kMaxDecompressedSize = 32 << 20;
constexpr td::uint32 kMaxFetchAttempts = 80;
constexpr double kQueryTimeout = 7.0;
constexpr size_t kPeersPerAttempt = 3;
constexpr td::uint32 kMaxPeersRequested = 20;
constexpr double kEmptyPeerRetry = 1.0;
constexpr double kPeerRefreshInterval = 30.0;
constexpr td::uint64 kMaxCacheBytes = 256ull << 20;

double fetch_backoff(td::uint32 attempts) {
  return std::min(0.5 + 0.25 * attempts, 4.0);
}

class OverlayCallback : public ton::overlay::Overlays::Callback {
 public:
  explicit OverlayCallback(td::actor::ActorId<OverlaySource> parent) : parent_(std::move(parent)) {
  }
  void receive_query(ton::adnl::AdnlNodeIdShort src, ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                     td::Promise<td::BufferSlice> promise) override {
    promise.set_error(td::Status::Error("ton-librarian does not serve overlay queries"));
  }
  void receive_broadcast(ton::PublicKeyHash src, ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data) override {
    td::actor::send_closure(parent_, &OverlaySource::on_broadcast, overlay_id, std::move(data));
  }

 private:
  td::actor::ActorId<OverlaySource> parent_;
};

// Parses a tonNode_DataFull answer into (block id, block data BoC).
td::Result<std::pair<ton::BlockIdExt, td::BufferSlice>> parse_data_full(td::BufferSlice data) {
  TRY_RESULT(f, ton::fetch_tl_object<ton::ton_api::tonNode_DataFull>(std::move(data), true));
  if (f->get_id() == ton::ton_api::tonNode_dataFullEmpty::ID) {
    return td::Status::Error("peer does not have this block");
  }
  if (f->get_id() == ton::ton_api::tonNode_dataFullCompressedV2::ID) {
    return td::Status::Error("tonNode_dataFullCompressedV2 is not supported");
  }
  if (f->get_id() == ton::ton_api::tonNode_dataFull::ID) {
    auto& x = static_cast<ton::ton_api::tonNode_dataFull&>(*f);
    return std::make_pair(ton::create_block_id(x.id_), std::move(x.block_));
  }
  auto& x = static_cast<ton::ton_api::tonNode_dataFullCompressed&>(*f);
  auto block_id = ton::create_block_id(x.id_);
  TRY_RESULT(decompressed, td::lz4_decompress(x.compressed_, kMaxDecompressedSize));
  TRY_RESULT(roots, vm::std_boc_deserialize_multi(decompressed, 2));
  if (roots.size() != 2) {
    return td::Status::Error("expected 2 roots in decompressed block data");
  }
  TRY_RESULT(block, vm::std_boc_serialize(roots[1], 31));
  return std::make_pair(block_id, std::move(block));
}

}  // namespace

td::Result<td::actor::ActorOwn<OverlaySource>> OverlaySource::create(const Config::OverlayConf& conf) {
  TRY_RESULT_PREFIX(data, td::read_file(conf.global_config_path), "failed to read global config: ");
  TRY_RESULT_PREFIX(json, td::json_decode(data.as_slice()), "failed to parse global config json: ");
  ton::ton_api::config_global gc;
  TRY_STATUS_PREFIX(ton::ton_api::from_json(gc, json.get_object()), "global config does not fit TL scheme: ");

  ton::adnl::AdnlNodesList static_nodes;
  if (gc.adnl_ && gc.adnl_->static_nodes_) {
    TRY_RESULT_PREFIX_ASSIGN(static_nodes, ton::adnl::AdnlNodesList::create(gc.adnl_->static_nodes_),
                             "bad static adnl nodes: ");
  }
  std::shared_ptr<ton::dht::DhtGlobalConfig> dht_config;
  if (gc.dht_) {
    TRY_RESULT_PREFIX_ASSIGN(dht_config, ton::dht::Dht::create_global_config(std::move(gc.dht_)), "bad dht config: ");
  }
  if (conf.use_public_overlays && !dht_config) {
    return td::Status::Error("global config has no dht section, which is required for public overlays");
  }

  // Chain identity (zero state) is required: overlay ids are derived from its
  // file hash. The catch-up anchor is validator.init_block (like
  // validator-engine does), falling back to the zero state.
  if (!(gc.validator_ && gc.validator_->zero_state_)) {
    return td::Status::Error("global config has no validator.zero_state (required for overlay ids)");
  }
  ton::BlockIdExt zero_state = ton::create_block_id(gc.validator_->zero_state_);
  ton::BlockIdExt start_block = zero_state;
  if (gc.validator_->init_block_) {
    start_block = ton::create_block_id(gc.validator_->init_block_);
  }
  ton::Bits256 zero_state_file_hash = zero_state.file_hash;

  auto key_path = conf.net_db + "/librarian-adnl";
  // NB: td::mkpath(path) creates every component BEFORE a slash (it treats
  // `path` as a file name), so "ton-db" alone would be a no-op; passing the
  // key file path makes it create the net_db directory itself.
  TRY_STATUS_PREFIX(td::mkpath(key_path, 0755), "cannot create net db directory: ");
  td::Bits256 key_data;
  auto r_key = td::read_file(key_path);
  if (r_key.is_ok() && r_key.ok().size() == 32) {
    key_data.as_slice().copy_from(r_key.ok().as_slice());
  } else {
    td::Random::secure_bytes(key_data.as_slice());
    TRY_STATUS_PREFIX(td::write_file(key_path, key_data.as_slice()), "cannot save adnl key: ");
  }
  ton::privkeys::Ed25519 private_key{key_data};

  return td::actor::create_actor<OverlaySource>("overlay-source", conf, zero_state_file_hash, start_block,
                                                std::move(static_nodes), std::move(dht_config),
                                                std::move(private_key));
}

OverlaySource::OverlaySource(Config::OverlayConf conf, ton::Bits256 zero_state_file_hash, ton::BlockIdExt init_block,
                             ton::adnl::AdnlNodesList static_nodes,
                             std::shared_ptr<ton::dht::DhtGlobalConfig> dht_config,
                             ton::privkeys::Ed25519 private_key)
    : conf_(std::move(conf))
    , zero_state_file_hash_(zero_state_file_hash)
    , init_block_(init_block)
    , static_nodes_(std::move(static_nodes))
    , dht_config_(std::move(dht_config))
    , private_key_(private_key) {
}

void OverlaySource::start_up() {
  keyring_ = ton::keyring::Keyring::create(conf_.net_db);
  network_manager_ = ton::adnl::AdnlNetworkManager::create();
  adnl_ = ton::adnl::Adnl::create(conf_.net_db, keyring_.get());
  td::actor::send_closure(adnl_, &ton::adnl::Adnl::register_network_manager, network_manager_.get());

  td::IPAddress listen_ip;
  listen_ip.init_host_port(conf_.listen_addr).ensure();
  ton::adnl::AdnlCategoryMask cat_mask;  // all categories
  for (size_t i = 0; i < cat_mask.size(); i++) {
    cat_mask.set(i);
  }
  td::actor::send_closure(network_manager_, &ton::adnl::AdnlNetworkManager::add_self_addr, listen_ip, cat_mask, 0);

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) {
    td::actor::send_closure(SelfId, &OverlaySource::on_key_added, std::move(R));
  });
  td::actor::send_closure(keyring_, &ton::keyring::Keyring::add_key, ton::PrivateKey{private_key_}, false,
                          std::move(P));
}

void OverlaySource::tear_down() {
  for (auto& [id, f] : fetches_) {
    if (f.next_block) {
      f.next_promise.set_error(td::Status::Error("overlay source stopped"));
    } else {
      f.block_promise.set_error(td::Status::Error("overlay source stopped"));
    }
  }
  fetches_.clear();
}

void OverlaySource::on_key_added(td::Result<td::Unit> R) {
  if (R.is_error()) {
    LOG(ERROR) << "failed to add adnl key: " << R.move_as_error();
    stop();
    return;
  }
  local_id_full_ = ton::adnl::AdnlNodeIdFull{private_key_.pub()};
  local_id_ = local_id_full_.compute_short_id();
  LOG(INFO) << "[overlay] adnl id: " << local_id_;

  td::IPAddress listen_ip;
  listen_ip.init_host_port(conf_.listen_addr).ensure();
  ton::adnl::AdnlAddressList addr_list;
  addr_list.add_udp_adnl_address(listen_ip).ensure();
  addr_list.set_version(0);
  addr_list.set_reinit_date(ton::adnl::Adnl::adnl_start_time());
  td::actor::send_closure(adnl_, &ton::adnl::Adnl::add_id, local_id_full_, addr_list, 0);

  auto static_nodes = std::move(static_nodes_);
  if (!static_nodes.nodes().empty()) {
    td::actor::send_closure(adnl_, &ton::adnl::Adnl::add_static_nodes_from_config, std::move(static_nodes));
  }

  if (dht_config_) {
    auto res = ton::dht::Dht::create_client(local_id_, conf_.net_db, dht_config_, keyring_.get(), adnl_.get());
    if (res.is_error()) {
      LOG(FATAL) << "failed to create dht client: " << res.move_as_error();
    }
    dht_ = res.move_as_ok();
    td::actor::send_closure(adnl_, &ton::adnl::Adnl::register_dht_node, dht_.get());
  }

  rldp2_ = ton::rldp2::Rldp::create(adnl_.get());
  auto peer_table = td::actor::actor_dynamic_cast<ton::adnl::AdnlPeerTable>(adnl_.get());
  CHECK(!peer_table.empty());
  quic_ = td::actor::create_actor<ton::quic::QuicSender>("QuicSender", peer_table, keyring_.get());

  overlays_ = ton::overlay::Overlays::create(conf_.net_db, keyring_.get(), adnl_.get(), dht_.get(),
                                             ton::overlay::OverlayManagerBufferLimits{1024, 2ull << 20});

  // register our id on both transport senders
  td::actor::send_closure(rldp2_.get(), &ton::rldp2::Rldp::add_id, local_id_);
  td::actor::send_closure(quic_.get(), &ton::quic::QuicSender::add_id, local_id_);

  create_mc_overlay();
  create_custom_overlays();

  next_peer_refresh_ = td::Timestamp::in(3.0);
  alarm_timestamp() = td::Timestamp::now();
}

td::actor::ActorId<ton::adnl::AdnlSenderEx> OverlaySource::sender_ex(bool use_quic) {
  return use_quic ? td::actor::ActorId<ton::adnl::AdnlSenderEx>{quic_.get()}
                  : td::actor::ActorId<ton::adnl::AdnlSenderEx>{rldp2_.get()};
}

td::actor::ActorId<ton::adnl::AdnlSenderInterface> OverlaySource::sender_for(
    ton::overlay::OverlayIdShort overlay_id) {
  bool use_quic = conf_.use_quic;
  for (auto& co : custom_overlays_) {
    if (co.id_short.bits256_value() == overlay_id.bits256_value()) {
      use_quic = co.use_quic;
      break;
    }
  }
  return td::actor::ActorId<ton::adnl::AdnlSenderInterface>{sender_ex(use_quic)};
}

void OverlaySource::create_mc_overlay() {
  if (!conf_.use_public_overlays) {
    return;
  }
  auto X = ton::create_hash_tl_object<ton::ton_api::tonNode_shardPublicOverlayId>(
      static_cast<td::int32>(ton::masterchainId), ton::shardIdAll, zero_state_file_hash_);
  td::BufferSlice b{32};
  b.as_slice().copy_from(X.as_slice());
  mc_overlay_full_ = ton::overlay::OverlayIdFull{std::move(b)};
  mc_overlay_short_ = mc_overlay_full_.compute_short_id();

  ton::overlay::OverlayOptions opts;
  opts.name_ = "masterchain";
  opts.announce_self_ = true;
  if (conf_.use_plumtree) {
    opts.enable_plumtree_broadcast_ = true;
    opts.plumtree_broadcast_sender_ = sender_ex(conf_.use_quic);
  }
  td::actor::send_closure(
      overlays_, &ton::overlay::Overlays::create_public_overlay_ex, local_id_, mc_overlay_full_.clone(),
      std::make_unique<OverlayCallback>(actor_id(this)),
      ton::overlay::OverlayPrivacyRules{ton::overlay::Overlays::max_fec_broadcast_size()},
      PSTRING() << "{ \"type\": \"shard\", \"shard_id\": " << ton::shardIdAll
                << ", \"workchain_id\": " << ton::masterchainId << " }",
      opts);
  request_peers(mc_overlay_short_);
}

void OverlaySource::create_custom_overlays() {
  for (const auto& co : conf_.custom_overlays) {
    CustomOverlay c;
    c.name = co.name;
    c.use_quic = co.use_quic;
    std::set<ton::adnl::AdnlNodeIdShort> nodes;
    for (const auto& node : co.nodes) {
      nodes.insert(ton::adnl::AdnlNodeIdShort{node.adnl_id});
    }
    c.nodes.assign(nodes.begin(), nodes.end());
    c.sender_shards = co.sender_shards;

    std::vector<td::Bits256> node_ids;
    for (auto& n : c.nodes) {
      node_ids.push_back(n.bits256_value());
    }
    auto X = ton::create_hash_tl_object<ton::ton_api::tonNode_customOverlayId>(zero_state_file_hash_, c.name,
                                                                               std::move(node_ids));
    td::BufferSlice b{32};
    b.as_slice().copy_from(X.as_slice());
    c.id_full = ton::overlay::OverlayIdFull{std::move(b)};
    c.id_short = c.id_full.compute_short_id();

    // authorize the listed nodes to send (fec) broadcasts to us
    std::map<ton::PublicKeyHash, td::uint32> authorized;
    for (auto& n : c.nodes) {
      authorized[n.pubkey_hash()] = ton::overlay::Overlays::max_fec_broadcast_size();
    }
    ton::overlay::OverlayPrivacyRules rules{ton::overlay::Overlays::max_fec_broadcast_size(), 0, std::move(authorized)};

    ton::overlay::OverlayOptions opts;
    opts.name_ = "custom." + c.name;
    opts.send_twostep_broadcast_ = true;
    opts.twostep_broadcast_sender_ = sender_ex(c.use_quic);

    td::actor::send_closure(
        overlays_, &ton::overlay::Overlays::create_private_overlay_ex, local_id_, c.id_full.clone(), c.nodes,
        std::make_unique<OverlayCallback>(actor_id(this)), rules,
        PSTRING() << R"({ "type": "custom-overlay", "name": ")" << td::format::Escaped{c.name} << R"(" })", opts);

    LOG(INFO) << "[overlay] joined custom overlay '" << c.name << "' with " << c.nodes.size() << " nodes";
    custom_overlays_.push_back(std::move(c));
  }
}

ton::overlay::OverlayIdShort OverlaySource::ensure_shard_overlay(const ton::ShardIdFull& shard) {
  auto it = shard_overlays_.find(shard);
  if (it != shard_overlays_.end()) {
    return it->second;
  }
  auto X = ton::create_hash_tl_object<ton::ton_api::tonNode_shardPublicOverlayId>(
      shard.workchain, shard.shard, zero_state_file_hash_);
  td::BufferSlice b{32};
  b.as_slice().copy_from(X.as_slice());
  ton::overlay::OverlayIdFull id_full{std::move(b)};
  auto short_id = id_full.compute_short_id();

  ton::overlay::OverlayOptions opts;
  opts.name_ = PSTRING() << "shard" << shard;
  opts.announce_self_ = true;
  if (conf_.use_plumtree) {
    opts.enable_plumtree_broadcast_ = true;
    opts.plumtree_broadcast_sender_ = sender_ex(conf_.use_quic);
  }
  td::actor::send_closure(
      overlays_, &ton::overlay::Overlays::create_public_overlay_ex, local_id_, id_full.clone(),
      std::make_unique<OverlayCallback>(actor_id(this)),
      ton::overlay::OverlayPrivacyRules{ton::overlay::Overlays::max_fec_broadcast_size()},
      PSTRING() << "{ \"type\": \"shard\", \"shard_id\": " << shard.shard << ", \"workchain_id\": " << shard.workchain
                << " }",
      opts);
  shard_overlays_[shard] = short_id;
  request_peers(short_id);
  return short_id;
}

void OverlaySource::update_shards(std::vector<ton::ShardIdFull> shards) {
  if (!conf_.use_public_overlays || local_id_.is_zero()) {
    return;
  }
  for (auto& s : shards) {
    if (!s.is_masterchain()) {
      ensure_shard_overlay(s);
    }
  }
  std::vector<ton::ShardIdFull> to_delete;
  for (auto& [shard, short_id] : shard_overlays_) {
    bool needed = false;
    for (auto& s : shards) {
      if (ton::shard_intersects(shard, s)) {
        needed = true;
        break;
      }
    }
    if (!needed) {
      for (auto& [id, f] : fetches_) {
        ton::ShardIdFull f_shard{f.block_id.id.workchain, f.block_id.id.shard};
        if (ton::shard_intersects(shard, f_shard)) {
          needed = true;
          break;
        }
      }
    }
    if (!needed) {
      to_delete.push_back(shard);
    }
  }
  for (auto& s : to_delete) {
    auto it = shard_overlays_.find(s);
    LOG(INFO) << "[overlay] leaving public overlay for " << s;
    td::actor::send_closure(overlays_, &ton::overlay::Overlays::delete_overlay, local_id_, it->second);
    peers_.erase(it->second);
    shard_overlays_.erase(it);
  }
}

// ---------------------------------------------------------------------------
// Fetch machinery.
// ---------------------------------------------------------------------------

void OverlaySource::get_block(const ton::BlockIdExt& id, td::Promise<td::BufferSlice> promise) {
  if (const td::BufferSlice* cached = lookup_cache(id)) {
    stats_.blocks_from_cache++;
    promise.set_value(cached->clone());
    return;
  }
  Fetch fetch;
  fetch.next_block = false;
  fetch.block_id = id;
  fetch.block_promise = std::move(promise);
  start_fetch(std::move(fetch));
}

void OverlaySource::get_next_mc_block(const ton::BlockIdExt& prev, td::Promise<ton::BlockIdExt> promise) {
  auto it = mc_cache_by_seqno_.find(prev.id.seqno + 1);
  if (it != mc_cache_by_seqno_.end()) {
    if (const td::BufferSlice* cached = lookup_cache(it->second)) {
      stats_.blocks_from_cache++;
      promise.set_value(ton::BlockIdExt{it->second});
      return;
    }
    mc_cache_by_seqno_.erase(it);
  }
  Fetch fetch;
  fetch.next_block = true;
  fetch.block_id = prev;
  fetch.next_promise = std::move(promise);
  start_fetch(std::move(fetch));
}

void OverlaySource::start_fetch(Fetch fetch) {
  auto id = next_fetch_id_++;
  fetch.id = id;
  auto res = fetches_.emplace(id, std::move(fetch));
  CHECK(res.second);
  pump_fetch(id);
}

std::vector<ton::overlay::OverlayIdShort> OverlaySource::candidate_overlays(const ton::BlockIdExt& id) {
  std::vector<ton::overlay::OverlayIdShort> res;
  ton::ShardIdFull shard{id.id.workchain, id.id.shard};
  for (auto& co : custom_overlays_) {
    if (co.serves_shard(shard)) {
      res.push_back(co.id_short);
    }
  }
  if (conf_.use_public_overlays) {
    if (id.is_masterchain()) {
      if (!mc_overlay_short_.bits256_value().is_zero()) {
        res.push_back(mc_overlay_short_);
      }
    } else {
      res.push_back(ensure_shard_overlay(shard));
    }
  }
  return res;
}

void OverlaySource::pump_fetch(td::uint64 fetch_id) {
  auto it = fetches_.find(fetch_id);
  if (it == fetches_.end()) {
    return;
  }
  auto& fetch = it->second;
  if (fetch.in_flight > 0) {
    return;
  }
  if (fetch.attempts >= kMaxFetchAttempts) {
    fail_fetch(fetch_id, td::Status::Error(PSLICE() << "failed to fetch " << fetch.block_id.id.to_str() << ": "
                                                    << (fetch.last_error.empty() ? "no peers" : fetch.last_error)));
    return;
  }
  if (local_id_.is_zero() || overlays_.empty()) {
    // network stack is still starting up; still count the round so that a
    // fetch can never wait forever (hybrid falls back to liteservers)
    fetch.attempts++;
    fetch.retry_at = td::Timestamp::in(0.5);
    alarm_timestamp().relax(fetch.retry_at);
    return;
  }

  auto overlays = candidate_overlays(fetch.block_id);
  struct Cand {
    ton::overlay::OverlayIdShort overlay;
    ton::adnl::AdnlNodeIdShort peer;
  };
  std::vector<Cand> cands;
  std::set<ton::adnl::AdnlNodeIdShort> picked;
  for (auto ov : overlays) {
    auto& pl = peers_[ov];
    if (pl.peers.empty()) {
      request_peers(ov);
      continue;
    }
    size_t base = fetch.peer_offset % pl.peers.size();
    for (size_t k = 0; k < pl.peers.size() && cands.size() < kPeersPerAttempt; k++) {
      auto& p = pl.peers[(base + k) % pl.peers.size()];
      if (p == local_id_) {
        continue;
      }
      if (picked.insert(p).second) {
        cands.push_back({ov, p});
      }
    }
  }

  if (cands.empty()) {
    // No peers known yet (peer discovery may be slow or broken): count the
    // round as an attempt, otherwise the fetch would retry forever and the
    // hybrid liteserver fallback would never trigger.
    fetch.attempts++;
    fetch.retry_at = td::Timestamp::in(0.5);
    alarm_timestamp().relax(fetch.retry_at);
    return;
  }

  fetch.attempts++;
  fetch.peer_offset += static_cast<td::uint32>(cands.size());
  auto query =
      fetch.next_block
          ? ton::create_serialize_tl_object<ton::ton_api::tonNode_downloadNextBlockFull>(
                ton::create_tl_block_id(fetch.block_id))
          : ton::create_serialize_tl_object<ton::ton_api::tonNode_downloadBlockFull>(
                ton::create_tl_block_id(fetch.block_id));
  for (auto& c : cands) {
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), fetch_id, peer = c.peer](td::Result<td::BufferSlice> R) {
          td::actor::send_closure(SelfId, &OverlaySource::on_query_answer, fetch_id, peer, std::move(R));
        });
    td::actor::send_closure(overlays_, &ton::overlay::Overlays::send_query_via, c.peer, local_id_, c.overlay,
                            "downloadBlockFull", std::move(P), td::Timestamp::in(kQueryTimeout), query.clone(),
                            kMaxAnswerSize, sender_for(c.overlay));
    stats_.queries_sent++;
    fetch.in_flight++;
  }
}

void OverlaySource::on_query_answer(td::uint64 fetch_id, ton::adnl::AdnlNodeIdShort peer,
                                     td::Result<td::BufferSlice> R) {
  auto it = fetches_.find(fetch_id);
  if (it == fetches_.end()) {
    return;  // resolved by a parallel answer
  }
  auto& fetch = it->second;
  CHECK(fetch.in_flight > 0);
  fetch.in_flight--;

  auto retry = [&]() {
    if (fetch.in_flight == 0) {
      fetch.retry_at = td::Timestamp::in(fetch_backoff(fetch.attempts));
      alarm_timestamp().relax(fetch.retry_at);
    }
  };

  auto parsed = [&]() -> td::Result<std::pair<ton::BlockIdExt, td::BufferSlice>> {
    TRY_RESULT(data, std::move(R));
    return parse_data_full(std::move(data));
  }();
  if (parsed.is_error()) {
    fetch.last_error = parsed.error().to_string();
    retry();
    return;
  }
  auto [answer_id, block_data] = parsed.move_as_ok();

  if (td::sha256_bits256(block_data.as_slice()) != answer_id.file_hash) {
    fetch.last_error = "block data hash mismatch";
    retry();
    return;
  }

  if (fetch.next_block) {
    if (!answer_id.is_masterchain() || answer_id.id.seqno != fetch.block_id.id.seqno + 1) {
      fetch.last_error = PSTRING() << "unexpected next masterchain block " << answer_id.id.seqno << " (expected "
                                   << fetch.block_id.id.seqno + 1 << ")";
      retry();
      return;
    }
    finish_fetch_next(fetch_id, answer_id, std::move(block_data));
  } else {
    if (!(answer_id == fetch.block_id)) {
      fetch.last_error = "peer returned data for a different block";
      retry();
      return;
    }
    finish_fetch_block(fetch_id, answer_id, std::move(block_data));
  }
}

void OverlaySource::finish_fetch_block(td::uint64 fetch_id, ton::BlockIdExt id, td::BufferSlice data) {
  auto it = fetches_.find(fetch_id);
  if (it == fetches_.end()) {
    return;
  }
  auto fetch = std::move(it->second);
  fetches_.erase(it);
  stats_.blocks_fetched++;
  cache_block(id, data.clone());
  fetch.block_promise.set_value(std::move(data));
}

void OverlaySource::finish_fetch_next(td::uint64 fetch_id, ton::BlockIdExt id, td::BufferSlice data) {
  auto it = fetches_.find(fetch_id);
  if (it == fetches_.end()) {
    return;
  }
  auto fetch = std::move(it->second);
  fetches_.erase(it);
  stats_.blocks_fetched++;
  cache_block(id, std::move(data));
  fetch.next_promise.set_value(std::move(id));
}

void OverlaySource::fail_fetch(td::uint64 fetch_id, td::Status error) {
  auto it = fetches_.find(fetch_id);
  if (it == fetches_.end()) {
    return;
  }
  auto fetch = std::move(it->second);
  fetches_.erase(it);
  stats_.fetch_failures++;
  LOG(WARNING) << "[overlay] " << error;
  if (fetch.next_block) {
    fetch.next_promise.set_error(std::move(error));
  } else {
    fetch.block_promise.set_error(std::move(error));
  }
}

// ---------------------------------------------------------------------------
// Peers.
// ---------------------------------------------------------------------------

void OverlaySource::request_peers(ton::overlay::OverlayIdShort overlay_id) {
  if (local_id_.is_zero() || overlays_.empty()) {
    return;
  }
  auto& pl = peers_[overlay_id];
  if (pl.querying || !pl.refreshed_at.is_in_past()) {
    return;
  }
  pl.querying = true;
  auto P = td::PromiseCreator::lambda(
      [SelfId = actor_id(this), overlay_id](td::Result<std::vector<ton::adnl::AdnlNodeIdShort>> R) {
        td::actor::send_closure(SelfId, &OverlaySource::on_peers, overlay_id, std::move(R));
      });
  td::actor::send_closure(overlays_, &ton::overlay::Overlays::get_overlay_random_peers, local_id_, overlay_id,
                          kMaxPeersRequested, std::move(P));
}

void OverlaySource::on_peers(ton::overlay::OverlayIdShort overlay_id,
                             td::Result<std::vector<ton::adnl::AdnlNodeIdShort>> peers) {
  auto& pl = peers_[overlay_id];
  pl.querying = false;
  if (peers.is_error()) {
    pl.peers.clear();
    pl.refreshed_at = td::Timestamp::in(kEmptyPeerRetry);
    return;
  }
  pl.peers = peers.move_as_ok();
  pl.peers.erase(std::remove_if(pl.peers.begin(), pl.peers.end(), [this](const ton::adnl::AdnlNodeIdShort& p) {
                   return p == local_id_;
                 }),
                 pl.peers.end());
  pl.refreshed_at = td::Timestamp::in(pl.peers.empty() ? kEmptyPeerRetry : kPeerRefreshInterval);
}

// ---------------------------------------------------------------------------
// Broadcast cache.
// ---------------------------------------------------------------------------

void OverlaySource::on_broadcast(ton::overlay::OverlayIdShort overlay_id, td::BufferSlice data) {
  stats_.broadcasts_received++;
  auto B = ton::fetch_tl_object<ton::ton_api::tonNode_Broadcast>(std::move(data), true);
  if (B.is_error()) {
    return;
  }
  auto b_ptr = B.move_as_ok();  // owns the object; keep it alive for the whole handler
  auto& b = *b_ptr;
  if (b.get_id() == ton::ton_api::tonNode_blockBroadcast::ID) {
    auto& f = static_cast<ton::ton_api::tonNode_blockBroadcast&>(b);
    auto id = ton::create_block_id(f.id_);
    if (td::sha256_bits256(f.data_.as_slice()) != id.file_hash) {
      return;
    }
    stats_.broadcasts_cached++;
    cache_block(id, std::move(f.data_));
  } else if (b.get_id() == ton::ton_api::tonNode_blockBroadcastCompressed::ID) {
    auto& f = static_cast<ton::ton_api::tonNode_blockBroadcastCompressed&>(b);
    auto id = ton::create_block_id(f.id_);
    auto decompressed = td::lz4_decompress(f.compressed_, kMaxDecompressedSize);
    if (decompressed.is_error()) {
      return;
    }
    auto inner =
        ton::fetch_tl_object<ton::ton_api::tonNode_blockBroadcastCompressed_data>(decompressed.move_as_ok(), true);
    if (inner.is_error()) {
      return;
    }
    auto roots = vm::std_boc_deserialize_multi(inner.move_as_ok()->proof_data_, 2);
    if (roots.is_error()) {
      return;
    }
    auto rs = roots.move_as_ok();
    if (rs.size() != 2) {
      return;
    }
    auto data2 = vm::std_boc_serialize(rs[1], 31);
    if (data2.is_error()) {
      return;
    }
    auto d = data2.move_as_ok();
    if (td::sha256_bits256(d.as_slice()) != id.file_hash) {
      return;
    }
    stats_.broadcasts_cached++;
    cache_block(id, std::move(d));
  }
  // tonNode_blockBroadcastCompressedV2 is a notification only; the block is
  // pulled with tonNode_downloadBlockFull when needed.
}

void OverlaySource::cache_block(const ton::BlockIdExt& id, td::BufferSlice data) {
  if (block_cache_.count(id) != 0) {
    return;
  }
  cache_bytes_ += data.size();
  block_cache_[id] = std::move(data);
  cache_order_.push_back(id);
  if (id.is_masterchain()) {
    mc_cache_by_seqno_[id.id.seqno] = id;
  }
  while (cache_order_.size() > kMaxCachedBlocks || cache_bytes_ > kMaxCacheBytes) {
    auto front = cache_order_.front();
    if (front.is_masterchain()) {
      auto it = mc_cache_by_seqno_.find(front.id.seqno);
      if (it != mc_cache_by_seqno_.end() && it->second == front) {
        mc_cache_by_seqno_.erase(it);
      }
    }
    cache_bytes_ -= block_cache_[front].size();
    block_cache_.erase(front);
    cache_order_.pop_front();
  }
}

const td::BufferSlice* OverlaySource::lookup_cache(const ton::BlockIdExt& id) {
  auto it = block_cache_.find(id);
  if (it == block_cache_.end()) {
    return nullptr;
  }
  return &it->second;
}

void OverlaySource::alarm() {
  auto now = td::Timestamp::now();
  if (!fetches_.empty()) {
    std::vector<td::uint64> due;
    for (auto& [id, f] : fetches_) {
      if (f.in_flight == 0 && f.retry_at.is_in_past()) {
        due.push_back(id);
      }
    }
    for (auto id : due) {
      pump_fetch(id);
    }
  }
  if (next_peer_refresh_.is_in_past()) {
    std::vector<ton::overlay::OverlayIdShort> active;
    if (conf_.use_public_overlays && !mc_overlay_short_.bits256_value().is_zero()) {
      active.push_back(mc_overlay_short_);
    }
    for (auto& [shard, ov] : shard_overlays_) {
      active.push_back(ov);
    }
    for (auto& co : custom_overlays_) {
      active.push_back(co.id_short);
    }
    for (auto ov : active) {
      request_peers(ov);
    }
    next_peer_refresh_ = now + kPeerRefreshInterval;
  }
  td::Timestamp next = td::Timestamp::never();
  for (auto& [id, f] : fetches_) {
    next.relax(f.retry_at);
  }
  next.relax(next_peer_refresh_);
  alarm_timestamp().relax(next);
}

}  // namespace tonlibrarian
