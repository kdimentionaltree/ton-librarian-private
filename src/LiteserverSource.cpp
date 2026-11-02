/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "LiteserverSource.h"

#include "auto/tl/lite_api.h"
#include "auto/tl/ton_api_json.h"
#include "tl/tl_json.h"
#include "tl-utils/lite-utils.hpp"
#include "ton/lite-tl.hpp"
#include "ton/ton-io.hpp"
#include "td/utils/JsonBuilder.h"
#include "td/utils/filesystem.h"

namespace tonlibrarian {

namespace {

td::BufferSlice envelope(td::BufferSlice query) {
  return ton::serialize_tl_object(ton::create_tl_object<ton::lite_api::liteServer_query>(std::move(query)), true);
}

// Unwraps a liteserver answer: transport errors pass through, liteServer.error
// payloads become td::Status errors.
td::Result<td::BufferSlice> unwrap_answer(td::Result<td::BufferSlice> R) {
  TRY_RESULT(data, std::move(R));
  auto F = ton::fetch_tl_object<ton::lite_api::liteServer_error>(data.clone(), true);
  if (F.is_ok()) {
    auto f = F.move_as_ok();
    return td::Status::Error(f->code_, f->message_);
  }
  return std::move(data);
}

// Sends one query to the liteserver cluster with bounded retries and backoff.
// The parse callback consumes a successful answer; returning an error makes
// the query retried.
class RetryQuery : public td::actor::Actor {
 public:
  using ParseFn = std::function<td::Status(td::BufferSlice)>;

  RetryQuery(td::actor::ActorId<liteclient::ExtClient> client, std::string name, td::BufferSlice query,
             td::uint32 max_attempts, double timeout, double backoff, ParseFn parse, td::Promise<td::Unit> promise)
      : client_(client)
      , name_(std::move(name))
      , query_(std::move(query))
      , max_attempts_(max_attempts)
      , timeout_(timeout)
      , backoff_(backoff)
      , parse_(std::move(parse))
      , promise_(std::move(promise)) {
  }

 private:
  void start_up() override {
    do_send();
  }
  void alarm() override {
    do_send();
  }
  void do_send() {
    if (attempts_ >= max_attempts_) {
      if (promise_) {
        promise_.set_error(
            td::Status::Error(PSLICE() << "liteserver query '" << name_ << "' failed after " << attempts_
                                       << " attempts: " << (last_error_.empty() ? "timeout" : last_error_)));
      }
      stop();
      return;
    }
    attempts_++;
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::BufferSlice> R) {
      td::actor::send_closure(SelfId, &RetryQuery::got_answer, std::move(R));
    });
    td::actor::send_closure(client_, &liteclient::ExtClient::send_query, name_, envelope(query_.clone()),
                            td::Timestamp::in(timeout_), std::move(P));
  }
  void got_answer(td::Result<td::BufferSlice> R) {
    auto data = unwrap_answer(std::move(R));
    if (data.is_error()) {
      last_error_ = data.error().to_string();
      alarm_timestamp() = td::Timestamp::in(backoff_);
      return;
    }
    auto S = parse_(data.move_as_ok());
    if (S.is_ok()) {
      if (promise_) {
        promise_.set_value(td::Unit());
      }
      stop();
    } else {
      last_error_ = S.to_string();
      alarm_timestamp() = td::Timestamp::in(backoff_);
    }
  }

  td::actor::ActorId<liteclient::ExtClient> client_;
  std::string name_;
  td::BufferSlice query_;
  td::uint32 max_attempts_;
  double timeout_;
  double backoff_;
  ParseFn parse_;
  td::Promise<td::Unit> promise_;
  td::uint32 attempts_ = 0;
  std::string last_error_;
};

void spawn_retry(td::actor::ActorId<liteclient::ExtClient> client, std::string name, td::BufferSlice query,
                 td::uint32 max_attempts, double timeout, double backoff, RetryQuery::ParseFn parse,
                 td::Promise<td::Unit> promise) {
  // The actor manages its own lifetime (stops on completion).
  td::actor::create_actor<RetryQuery>(PSLICE() << "litequery." << name, client, name, std::move(query), max_attempts,
                                       timeout, backoff, std::move(parse), std::move(promise))
      .release();
}

}  // namespace

td::Result<std::vector<liteclient::LiteServerConfig>> LiteserverSource::parse_global_config(const std::string& path) {
  TRY_RESULT_PREFIX(data, td::read_file(path), "failed to read global config: ");
  TRY_RESULT_PREFIX(json, td::json_decode(data.as_slice()), "failed to parse global config json: ");
  ton::ton_api::liteclient_config_global gc;
  TRY_STATUS_PREFIX(ton::ton_api::from_json(gc, json.get_object()), "global config does not fit scheme: ");
  TRY_RESULT_PREFIX(servers, liteclient::LiteServerConfig::parse_global_config(gc),
                     "failed to parse liteservers: ");
  if (servers.empty()) {
    return td::Status::Error(PSLICE() << "no liteservers found in " << path);
  }
  return std::move(servers);
}

td::Result<td::actor::ActorOwn<LiteserverSource>> LiteserverSource::create(const std::string& global_config_path) {
  TRY_RESULT(servers, parse_global_config(global_config_path));
  return td::actor::create_actor<LiteserverSource>("liteserver-source", std::move(servers));
}

LiteserverSource::LiteserverSource(std::vector<liteclient::LiteServerConfig> servers) : servers_(std::move(servers)) {
}

void LiteserverSource::start_up() {
  client_ = liteclient::ExtClient::create(servers_, nullptr, true);
}

void LiteserverSource::get_masterchain_info(td::Promise<ton::BlockIdExt> promise) {
  auto res = std::make_shared<ton::BlockIdExt>();
  auto query = ton::create_serialize_tl_object<ton::lite_api::liteServer_getMasterchainInfo>();
  auto parse = [res](td::BufferSlice data) -> td::Status {
    TRY_RESULT(f, ton::fetch_tl_object<ton::lite_api::liteServer_masterchainInfo>(std::move(data), true));
    *res = ton::create_block_id(f->last_);
    return td::Status::OK();
  };
  spawn_retry(client_.get(), "getMasterchainInfo", std::move(query), 10, 15.0, 2.0, std::move(parse),
              [res, promise = std::move(promise)](td::Result<td::Unit> R) mutable {
                if (R.is_error()) {
                  promise.set_error(R.move_as_error());
                } else {
                  promise.set_value(std::move(*res));
                }
              });
}

void LiteserverSource::lookup_mc_block(td::uint32 seqno, td::Promise<ton::BlockIdExt> promise) {
  auto res = std::make_shared<ton::BlockIdExt>();
  auto query = ton::create_serialize_tl_object<ton::lite_api::liteServer_lookupBlock>(
      1, ton::create_tl_lite_block_id_simple(ton::BlockId{ton::masterchainId, ton::shardIdAll, seqno}), 0, 0);
  auto parse = [res, seqno](td::BufferSlice data) -> td::Status {
    TRY_RESULT(f, ton::fetch_tl_object<ton::lite_api::liteServer_blockHeader>(std::move(data), true));
    *res = ton::create_block_id(f->id_);
    if (res->id.seqno != seqno) {
      return td::Status::Error(PSLICE() << "lookupBlock returned wrong seqno " << res->id.seqno);
    }
    return td::Status::OK();
  };
  spawn_retry(client_.get(), "lookupBlock", std::move(query), 12, 15.0, 2.0, std::move(parse),
              [res, promise = std::move(promise)](td::Result<td::Unit> R) mutable {
                if (R.is_error()) {
                  promise.set_error(R.move_as_error());
                } else {
                  promise.set_value(std::move(*res));
                }
              });
}

void LiteserverSource::get_block(const ton::BlockIdExt& id, td::Promise<td::BufferSlice> promise) {
  auto res = std::make_shared<td::BufferSlice>();
  auto query = ton::create_serialize_tl_object<ton::lite_api::liteServer_getBlock>(ton::create_tl_lite_block_id(id));
  auto parse = [res, id](td::BufferSlice data) -> td::Status {
    TRY_RESULT(f, ton::fetch_tl_object<ton::lite_api::liteServer_blockData>(std::move(data), true));
    if (ton::create_block_id(f->id_) != id) {
      return td::Status::Error("getBlock returned data for a wrong block");
    }
    *res = std::move(f->data_);
    return td::Status::OK();
  };
  spawn_retry(client_.get(), "getBlock", std::move(query), 10, 30.0, 3.0, std::move(parse),
              [res, promise = std::move(promise)](td::Result<td::Unit> R) mutable {
                if (R.is_error()) {
                  promise.set_error(R.move_as_error());
                } else {
                  promise.set_value(std::move(*res));
                }
              });
}

void LiteserverSource::get_account_state(const ton::BlockIdExt& id, const Account& account,
                                         td::Promise<td::BufferSlice> promise) {
  auto res = std::make_shared<td::BufferSlice>();
  auto account_id = ton::create_tl_object<ton::lite_api::liteServer_accountId>(static_cast<std::int32_t>(account.wc),
                                                                               account.addr);
  auto query = ton::create_serialize_tl_object<ton::lite_api::liteServer_getAccountState>(
      ton::create_tl_lite_block_id(id), std::move(account_id));
  auto parse = [res, id](td::BufferSlice data) -> td::Status {
    TRY_RESULT(f, ton::fetch_tl_object<ton::lite_api::liteServer_accountState>(std::move(data), true));
    if (ton::create_block_id(f->id_) != id) {
      return td::Status::Error("getAccountState returned data for a wrong block");
    }
    *res = std::move(f->state_);
    return td::Status::OK();
  };
  spawn_retry(client_.get(), "getAccountState", std::move(query), 10, 60.0, 5.0, std::move(parse),
              [res, promise = std::move(promise)](td::Result<td::Unit> R) mutable {
                if (R.is_error()) {
                  promise.set_error(R.move_as_error());
                } else {
                  promise.set_value(std::move(*res));
                }
              });
}

}  // namespace tonlibrarian
