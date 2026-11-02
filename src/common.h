/*
    ton-librarian: a TON blockchain indexer node on top of ton-blockchain/ton.

    Copyright (C) 2026 toncenter

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "crypto/common/refint.h"
#include "td/utils/Slice.h"
#include "td/utils/bits.h"
#include "td/utils/buffer.h"
#include "td/utils/misc.h"
#include "ton/ton-types.h"
#include "vm/boc.h"
#include "vm/cells.h"

namespace tonlibrarian {

using td::Bits256;

// ---------------------------------------------------------------------------
// Serialization primitives.
// Values are little-endian; keys that need ordered iteration are big-endian.
// ---------------------------------------------------------------------------

class SerError : public std::runtime_error {
 public:
  explicit SerError(std::string msg) : std::runtime_error(std::move(msg)) {
  }
};

class Writer {
 public:
  void u8(td::uint8 v) {
    buf_.push_back(static_cast<char>(v));
  }
  void i8(td::int8 v) {
    buf_.push_back(static_cast<char>(v));
  }
  void u16(td::uint16 v) {
    for (int i = 0; i < 2; i++) {
      u8(static_cast<td::uint8>(v >> (8 * i)));
    }
  }
  void u32(td::uint32 v) {
    for (int i = 0; i < 4; i++) {
      u8(static_cast<td::uint8>(v >> (8 * i)));
    }
  }
  void u64(td::uint64 v) {
    for (int i = 0; i < 8; i++) {
      u8(static_cast<td::uint8>(v >> (8 * i)));
    }
  }
  void i32(td::int32 v) {
    u32(static_cast<td::uint32>(v));
  }
  void i64(td::int64 v) {
    u64(static_cast<td::uint64>(v));
  }
  void varu64(td::uint64 v) {
    do {
      td::uint8 b = v & 0x7f;
      v >>= 7;
      if (v) {
        b |= 0x80;
      }
      u8(b);
    } while (v);
  }
  void vari64(td::int64 v) {
    varu64(static_cast<td::uint64>(v));
  }
  void raw(td::Slice s) {
    buf_.append(s.data(), s.size());
  }
  void varbytes(td::Slice s) {
    varu64(s.size());
    raw(s);
  }
  void bits256(const td::Bits256& v) {
    raw(v.as_slice());
  }
  void boolean(bool v) {
    u8(v ? 1 : 0);
  }
  const std::string& str() const {
    return buf_;
  }
  std::string take() {
    return std::move(buf_);
  }

 private:
  std::string buf_;
};

class Reader {
 public:
  Reader() = default;
  explicit Reader(td::Slice data) : data_(data) {
  }
  td::uint8 u8() {
    check(1);
    return static_cast<td::uint8>(data_.data()[pos_++]);
  }
  td::int8 i8() {
    return static_cast<td::int8>(u8());
  }
  td::uint16 u16() {
    td::uint16 v = 0;
    for (int i = 0; i < 2; i++) {
      v |= static_cast<td::uint16>(u8()) << (8 * i);
    }
    return v;
  }
  td::uint32 u32() {
    td::uint32 v = 0;
    for (int i = 0; i < 4; i++) {
      v |= static_cast<td::uint32>(u8()) << (8 * i);
    }
    return v;
  }
  td::uint64 u64() {
    td::uint64 v = 0;
    for (int i = 0; i < 8; i++) {
      v |= static_cast<td::uint64>(u8()) << (8 * i);
    }
    return v;
  }
  td::int32 i32() {
    return static_cast<td::int32>(u32());
  }
  td::int64 i64() {
    return static_cast<td::int64>(u64());
  }
  td::uint64 varu64() {
    td::uint64 v = 0;
    int shift = 0;
    while (true) {
      td::uint8 b = u8();
      v |= static_cast<td::uint64>(b & 0x7f) << shift;
      if (!(b & 0x80)) {
        break;
      }
      shift += 7;
      if (shift > 63) {
        throw SerError("varint too long");
      }
    }
    return v;
  }
  td::int64 vari64() {
    return static_cast<td::int64>(varu64());
  }
  td::Slice raw(size_t n) {
    check(n);
    td::Slice s{data_.data() + pos_, n};
    pos_ += n;
    return s;
  }
  td::Slice varbytes() {
    auto n = varu64();
    return raw(n);
  }
  td::Bits256 bits256() {
    td::Bits256 v;
    v.as_slice().copy_from(raw(32));
    return v;
  }
  bool boolean() {
    return u8() != 0;
  }
  bool eof() const {
    return pos_ >= data_.size();
  }
  size_t left() const {
    return data_.size() - pos_;
  }

 private:
  void check(size_t n) const {
    if (pos_ + n > data_.size()) {
      throw SerError("unexpected end of record");
    }
  }
  td::Slice data_;
  size_t pos_ = 0;
};

// ---------------------------------------------------------------------------
// Coins: a TON CurrencyCollection (grams + extra currencies).
// Wire format: u8 flag (0 = zero, 1 = fits int64, 2 = 32-byte big-endian
// two's complement), then optional payload; then u8 extra flag + BOC bytes.
// ---------------------------------------------------------------------------

struct Coins {
  td::RefInt256 grams;     // may be null (== zero)
  td::Ref<vm::Cell> extra;  // extra currencies dictionary, may be null

  Coins() = default;
  explicit Coins(td::RefInt256 g) : grams(std::move(g)) {
  }
  static Coins zero() {
    return Coins{td::RefInt256{true, 0}};
  }
  bool is_zero() const {
    return grams.is_null() || !td::sgn(grams);
  }
  bool has_extra() const {
    return extra.not_null();
  }

  void serialize(Writer& w) const {
    if (grams.is_null() || !td::sgn(grams)) {
      w.u8(0);
    } else {
      unsigned char buf[32];
      if (!grams->export_bytes(buf, 32, true)) {
        throw SerError("coins value does not fit into 32 bytes");
      }
      if (grams->signed_fits_bits(64)) {  // small enough: high 24 bytes are sign extension
        td::uint64 v = 0;
        for (int i = 24; i < 32; i++) {
          v = (v << 8) | buf[i];
        }
        w.u8(1);
        w.u64(v);
      } else {
        w.u8(2);
        w.raw(td::Slice(buf, 32));
      }
    }
    if (extra.not_null()) {
      auto r = vm::std_boc_serialize(extra, 31);
      if (r.is_error()) {
        throw SerError("cannot serialize extra currencies: " + r.error().to_string());
      }
      w.varbytes(r.move_as_ok().as_slice());
    } else {
      w.varbytes(td::Slice());
    }
  }

  static Coins deserialize(Reader& r) {
    Coins c;
    auto flag = r.u8();
    if (flag == 0) {
      c.grams = td::RefInt256{true, 0};
    } else if (flag == 1) {
      c.grams = td::make_refint(static_cast<td::int64>(r.u64()));
    } else if (flag == 2) {
      auto s = r.raw(32);
      td::BigInt256 v;
      if (!v.import_bytes(s.ubegin(), 32, true)) {
        throw SerError("cannot import int256");
      }
      c.grams = td::RefInt256{true, std::move(v)};
    } else {
      throw SerError("bad coins flag");
    }
    auto extra = r.varbytes();
    if (extra.size() > 0) {
      auto rc = vm::std_boc_deserialize(extra);
      if (rc.is_error()) {
        throw SerError("cannot deserialize extra currencies");
      }
      c.extra = rc.move_as_ok();
    }
    return c;
  }

  td::RefInt256 int_value() const {
    if (grams.not_null()) {
      return grams;
    }
    return td::RefInt256{true, 0};
  }
};

// ---------------------------------------------------------------------------
// RocksDB key codecs. Ordered keys are big-endian.
// ---------------------------------------------------------------------------

inline std::string block_key(ton::WorkchainId wc, td::uint64 shard, td::uint32 seqno) {
  std::string s;
  s.reserve(13);
  s.push_back(static_cast<char>(static_cast<td::int8>(wc)));
  for (int i = 7; i >= 0; i--) {
    s.push_back(static_cast<char>((shard >> (8 * i)) & 0xff));
  }
  for (int i = 3; i >= 0; i--) {
    s.push_back(static_cast<char>((seqno >> (8 * i)) & 0xff));
  }
  return s;
}

inline std::string block_key(const ton::BlockIdExt& id) {
  return block_key(id.id.workchain, id.id.shard, id.id.seqno);
}

inline std::string account_key(td::int8 wc, td::Slice addr) {
  std::string s;
  s.reserve(33);
  s.push_back(static_cast<char>(wc));
  s.append(addr.data(), addr.size());
  return s;
}

// account (33 bytes) + lt (u64 BE)
inline std::string tx_by_account_key(td::int8 wc, td::Slice addr, td::uint64 lt) {
  std::string s = account_key(wc, addr);
  for (int i = 7; i >= 0; i--) {
    s.push_back(static_cast<char>((lt >> (8 * i)) & 0xff));
  }
  return s;
}

// account (33) + direction (1) + lt (u64 BE)
inline std::string msg_by_account_key(td::int8 wc, td::Slice addr, td::uint8 dir, td::uint64 lt) {
  std::string s = account_key(wc, addr);
  s.push_back(static_cast<char>(dir));
  for (int i = 7; i >= 0; i--) {
    s.push_back(static_cast<char>((lt >> (8 * i)) & 0xff));
  }
  return s;
}

// tx_hash (32) + direction (1) + index (u16 BE)
inline std::string msg_link_key(const td::Bits256& tx_hash, td::uint8 dir, td::uint16 idx) {
  std::string s;
  s.reserve(35);
  s.append(tx_hash.as_slice().data(), 32);
  s.push_back(static_cast<char>(dir));
  s.push_back(static_cast<char>((idx >> 8) & 0xff));
  s.push_back(static_cast<char>(idx & 0xff));
  return s;
}

// block_key (13) + tx index (u32 BE)
inline std::string block_tx_key(const ton::BlockIdExt& id, td::uint32 idx) {
  std::string s = block_key(id);
  for (int i = 3; i >= 0; i--) {
    s.push_back(static_cast<char>((idx >> (8 * i)) & 0xff));
  }
  return s;
}

// mc_seqno (u32 BE) + wc (i8) + shard (u64 BE)
inline std::string shard_state_key(td::uint32 mc_seqno, ton::WorkchainId wc, td::uint64 shard) {
  std::string s;
  s.reserve(17);
  for (int i = 3; i >= 0; i--) {
    s.push_back(static_cast<char>((mc_seqno >> (8 * i)) & 0xff));
  }
  s.push_back(static_cast<char>(static_cast<td::int8>(wc)));
  for (int i = 7; i >= 0; i--) {
    s.push_back(static_cast<char>((shard >> (8 * i)) & 0xff));
  }
  return s;
}

inline std::string shard_state_prefix(td::uint32 mc_seqno) {
  std::string s;
  s.reserve(4);
  for (int i = 3; i >= 0; i--) {
    s.push_back(static_cast<char>((mc_seqno >> (8 * i)) & 0xff));
  }
  return s;
}

inline ton::BlockId parse_block_id_str(td::int8 wc, td::uint64 shard, td::uint32 seqno) {
  return ton::BlockId{static_cast<ton::WorkchainId>(wc), shard, seqno};
}

// Account identity: workchain + 256-bit address.
struct Account {
  td::int8 wc = 0;
  td::Bits256 addr{};

  Account() = default;
  Account(td::int8 wc, const td::Bits256& addr) : wc(wc), addr(addr) {
  }
  bool operator<(const Account& other) const {
    return wc != other.wc ? wc < other.wc : addr < other.addr;
  }
  bool operator==(const Account& other) const {
    return wc == other.wc && addr == other.addr;
  }
  std::string key() const {
    return account_key(wc, addr.as_slice());
  }
  // "wc:hex"
  std::string to_raw_string() const {
    return PSTRING() << static_cast<int>(wc) << ":" << addr.to_hex();
  }
};

// Parses "wc:hex64" (ton-indexer style raw address) or throws.
inline Account parse_account(const std::string& s) {
  auto pos = s.find(':');
  if (pos == std::string::npos || pos == 0 || pos + 1 >= s.size()) {
    throw SerError("address must look like wc:hex");
  }
  auto wc = td::to_integer<int>(s.substr(0, pos));
  auto hex = s.substr(pos + 1);
  if (hex.size() != 64) {
    throw SerError("address hex must be 64 characters");
  }
  td::Bits256 addr;
  if (addr.from_hex(hex) < 0) {
    throw SerError("bad address hex");
  }
  return Account{static_cast<td::int8>(wc), addr};
}

inline td::Bits256 parse_hash_hex(const std::string& s) {
  if (s.size() != 64) {
    throw SerError("hash must be 64 hex characters");
  }
  td::Bits256 h;
  if (h.from_hex(s) < 0) {
    throw SerError("bad hash hex");
  }
  return h;
}

}  // namespace tonlibrarian
