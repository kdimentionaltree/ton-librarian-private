/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "Config.h"

#include "auto/tl/ton_api_json.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/Status.h"
#include "td/utils/filesystem.h"
#include "tl/tl_json.h"

namespace tonlibrarian {

td::Result<Config> Config::parse_file(const std::string& path) {
  TRY_RESULT(data, td::read_file(path));
  TRY_RESULT_PREFIX(cfg, parse(data.as_slice()), PSLICE() << "failed to parse config " << path << ": ");
  TRY_STATUS_PREFIX(cfg.validate(), PSLICE() << "invalid config " << path << ": ");
  return std::move(cfg);
}

td::Result<Config> Config::parse(td::MutableSlice json_data) {
  TRY_RESULT(json, td::json_decode(json_data));
  if (json.type() != td::JsonValue::Type::Object) {
    return td::Status::Error("config root must be an object");
  }
  auto& obj = json.get_object();

  Config cfg;

  TRY_RESULT(db_path, obj.get_optional_string_field("db"));
  if (!db_path.empty()) {
    cfg.db_path = db_path;
  }
  TRY_RESULT(from_seqno, obj.get_optional_long_field("from_seqno", 0));
  cfg.from_seqno = static_cast<td::uint32>(from_seqno);
  TRY_RESULT(until_seqno, obj.get_optional_long_field("until_seqno", 0));
  cfg.until_seqno = static_cast<td::uint32>(until_seqno);

  // pipeline
  if (obj.has_field("pipeline")) {
    TRY_RESULT(pipeline_json, obj.extract_optional_field("pipeline", td::JsonValue::Type::Object));
    auto& po = pipeline_json.get_object();
    TRY_RESULT(depth, po.get_optional_int_field("depth", static_cast<td::int32>(cfg.pipeline.depth)));
    cfg.pipeline.depth = static_cast<td::uint32>(depth);
    TRY_RESULT(fetch_par, po.get_optional_int_field("block_fetch_parallelism",
                                                    static_cast<td::int32>(cfg.pipeline.block_fetch_parallelism)));
    cfg.pipeline.block_fetch_parallelism = static_cast<td::uint32>(fetch_par);
    TRY_RESULT(state_par, po.get_optional_int_field("state_fetch_parallelism",
                                                    static_cast<td::int32>(cfg.pipeline.state_fetch_parallelism)));
    cfg.pipeline.state_fetch_parallelism = static_cast<td::uint32>(state_par);
    TRY_RESULT(workers, po.get_optional_int_field("parse_workers", static_cast<td::int32>(cfg.pipeline.parse_workers)));
    cfg.pipeline.parse_workers = static_cast<td::uint32>(workers);
  }

  // account_states
  if (obj.has_field("account_states")) {
    TRY_RESULT(as_json, obj.extract_optional_field("account_states", td::JsonValue::Type::Object));
    auto& ao = as_json.get_object();
    TRY_RESULT(enabled, ao.get_optional_bool_field("enabled", cfg.account_states.enabled));
    cfg.account_states.enabled = enabled;
    TRY_RESULT(max_boc, ao.get_optional_long_field("max_data_boc_size",
                                                   static_cast<td::int64>(cfg.account_states.max_data_boc_size)));
    cfg.account_states.max_data_boc_size = static_cast<td::uint64>(max_boc);
  }

  // sync
  if (obj.has_field("sync")) {
    TRY_RESULT(sync_json, obj.extract_optional_field("sync", td::JsonValue::Type::Object));
    auto& so = sync_json.get_object();
    TRY_RESULT(mode, so.get_optional_string_field("mode"));
    if (!mode.empty()) {
      if (mode == "liteservers") {
        cfg.mode = SyncMode::Liteservers;
      } else if (mode == "overlay") {
        cfg.mode = SyncMode::Overlay;
      } else if (mode == "hybrid") {
        cfg.mode = SyncMode::Hybrid;
      } else {
        return td::Status::Error(PSLICE() << "unknown sync.mode '" << mode << "' (expected liteservers|overlay|hybrid)");
      }
    }
    if (so.has_field("liteservers")) {
      TRY_RESULT(ls_json, so.extract_optional_field("liteservers", td::JsonValue::Type::Object));
      auto& lo = ls_json.get_object();
      TRY_RESULT(gc, lo.get_optional_string_field("global_config_path"));
      cfg.liteservers.global_config_path = gc;
    }
    if (so.has_field("overlay")) {
      TRY_RESULT(ov_json, so.extract_optional_field("overlay", td::JsonValue::Type::Object));
      auto& oo = ov_json.get_object();
      TRY_RESULT(gc, oo.get_optional_string_field("global_config_path"));
      if (!gc.empty()) {
        cfg.overlay.global_config_path = gc;
      }
      TRY_RESULT(la, oo.get_optional_string_field("listen_addr"));
      if (!la.empty()) {
        cfg.overlay.listen_addr = la;
      }
      TRY_RESULT(nd, oo.get_optional_string_field("net_db"));
      if (!nd.empty()) {
        cfg.overlay.net_db = nd;
      }
      TRY_RESULT(upo, oo.get_optional_bool_field("use_public_overlays", cfg.overlay.use_public_overlays));
      cfg.overlay.use_public_overlays = upo;
      TRY_RESULT(quic, oo.get_optional_bool_field("quic", cfg.overlay.use_quic));
      cfg.overlay.use_quic = quic;
      TRY_RESULT(plumtree, oo.get_optional_bool_field("plumtree", cfg.overlay.use_plumtree));
      cfg.overlay.use_plumtree = plumtree;
      if (oo.has_field("custom_overlays")) {
        TRY_RESULT(co_json, oo.extract_optional_field("custom_overlays", td::JsonValue::Type::Array));
        for (auto& elem : co_json.get_array()) {
          if (elem.type() != td::JsonValue::Type::Object) {
            return td::Status::Error("sync.overlay.custom_overlays entries must be objects");
          }
          ton::ton_api::engine_validator_customOverlay overlay;
          TRY_STATUS_PREFIX(ton::ton_api::from_json(overlay, elem.get_object()),
                            "bad custom overlay definition: ");
          CustomOverlayConf co;
          co.name = std::move(overlay.name_);
          co.use_quic = overlay.use_quic_;
          for (const auto& node : overlay.nodes_) {
            co.nodes.push_back(CustomOverlayNodeConf{node->adnl_id_, node->msg_sender_,
                                                     node->msg_sender_priority_, node->block_sender_,
                                                     node->accept_queries_});
          }
          for (const auto& sh : overlay.sender_shards_) {
            co.sender_shards.emplace_back(ton::ShardIdFull{sh->workchain_, static_cast<td::uint64>(sh->shard_)});
          }
          cfg.overlay.custom_overlays.push_back(std::move(co));
        }
      }
    }
  }

  return std::move(cfg);
}

td::Status Config::validate() const {
  if (pipeline.depth < 1 || pipeline.depth > 128) {
    return td::Status::Error("pipeline.depth must be in [1, 128]");
  }
  if (pipeline.parse_workers < 1) {
    return td::Status::Error("pipeline.parse_workers must be >= 1");
  }
  switch (mode) {
    case SyncMode::Liteservers:
    case SyncMode::Hybrid:
      if (liteservers.global_config_path.empty()) {
        return td::Status::Error(
            "sync.liteservers.global_config_path is required for sync.mode liteservers/hybrid");
      }
      break;
    case SyncMode::Overlay:
      if (overlay.global_config_path.empty()) {
        return td::Status::Error("sync.overlay.global_config_path is required for sync.mode overlay");
      }
      if (account_states.enabled) {
        return td::Status::Error(
            "account_states.enabled is not supported with sync.mode overlay (account states are fetched from "
            "liteservers); use sync.mode hybrid or liteservers");
      }
      break;
  }
  return td::Status::OK();
}

}  // namespace tonlibrarian
