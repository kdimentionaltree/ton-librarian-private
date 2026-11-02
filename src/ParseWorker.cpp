/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#include "ParseWorker.h"

namespace tonlibrarian {

void ParseWorker::parse_block(ton::BlockIdExt id, td::BufferSlice data, td::Promise<ParsedBlock> promise) {
  promise.set_result(tonlibrarian::parse_block(id, std::move(data)));
}

void ParseWorker::parse_account_state(Account account, td::uint32 gen_utime, td::BufferSlice data,
                                      td::uint64 max_boc_size, td::Promise<ParsedAccountState> promise) {
  promise.set_result(
      tonlibrarian::parse_account_state(std::move(data), account, gen_utime, static_cast<size_t>(max_boc_size)));
}

}  // namespace tonlibrarian
