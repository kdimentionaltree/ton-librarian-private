/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include "BlockParser.h"
#include "td/actor/actor.h"
#include "td/utils/buffer.h"
#include "ton/ton-types.h"

namespace tonlibrarian {

// CPU worker actor: runs TL-B parsing off the coordinator thread. Several
// instances are placed on distinct scheduler threads (see LibrarianNode);
// requests are round-robined between them by the coordinator.
class ParseWorker : public td::actor::Actor {
 public:
  void parse_block(ton::BlockIdExt id, td::BufferSlice data, td::Promise<ParsedBlock> promise);
  void parse_account_state(Account account, td::uint32 gen_utime, td::BufferSlice data, td::uint64 max_boc_size,
                           td::Promise<ParsedAccountState> promise);
};

}  // namespace tonlibrarian
