# ton-librarian

A TON blockchain indexer node built on top of [ton-blockchain/ton](https://github.com/ton-blockchain/ton)
(an analog of [toncenter/ton-indexer](https://github.com/toncenter/ton-indexer)).

It syncs the blockchain and stores blocks / transactions / messages / account
states in RocksDB in a convenient, queryable way — with **no validation**:
the indexer trusts the masterchain and only reassembles data.

## Design

- **Atomic unit = masterchain slice** (like ton-indexer): the masterchain block
  plus every shard block produced under it. Slices are committed strictly in
  `mc_seqno` order with a single RocksDB `WriteBatch` per slice, so the
  database is always in a consistent, prefix-complete state.
- **Quick startup**: no blockchain validation; blocks are parsed (TL-B) as
  they arrive and the result is stored in pre-computed record formats.
- **Pipeline**: a coordinator actor fetches slices, dedicated parse workers
  (each pinned to its own scheduler thread) do the CPU-heavy TL-B parsing,
  and the coordinator commits in order. Up to `pipeline.depth` slices are
  kept in flight.
- **Slice reconstruction**: the set of shard blocks of slice `S` is the union
  of `tops(S)` (from `McBlockExtra.shard_hashes`) walked back through `prev`
  links until reaching `tops(S-1)` (loaded from the DB, or bootstrapped by
  fetching masterchain block `S-1`).
- **Account states**: for every account touched by the slice's transactions
  the final state is fetched from a liteserver (`liteServer.getAccountState`)
  at the slice's masterchain block. (Full shard states are only served for
  blocks with seqno ≤ 1000, so they cannot be used for live indexing.)

## Sync modes (`sync.mode`)

| mode | blocks | account states | notes |
|------|--------|----------------|-------|
| `liteservers` | liteservers | liteservers | simplest; works everywhere |
| `overlay` | validator overlays | **disabled** | no liteservers for account-state queries |
| `hybrid` | overlays + liteserver fallback | liteservers | best bandwidth use |

The overlay source reuses the ton networking stack (ADNL/DHT/RLDP/QUIC,
public shard overlays and/or private `custom_overlays` from the global
config) and fetches blocks with `tonNode_downloadBlockFull` /
`tonNode_downloadNextBlockFull`; received block broadcasts are cached and
used opportunistically.

## Building

Requires: CMake ≥ 3.16, Ninja, clang ≥ 19 (or gcc ≥ 13), and the ton
submodules:

```bash
git submodule update --init --recursive
cmake -B build -GNinja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
ninja -C build ton-librarian ton-librarian-query
```

## Running

```bash
./build/src/ton-librarian config.json
```

Config (see `config.example.json`):

```jsonc
{
  "db": "librarian-db",                // RocksDB directory
  "from_seqno": 0,                     // 0 = continue existing db / start at tip
  "until_seqno": 0,                    // 0 = run forever
  "sync": {
    "mode": "liteservers",             // liteservers | overlay | hybrid
    "liteservers": { "global_config_path": "configs/global.config.json" },
    "overlay": {
      "global_config_path": "configs/global.config.json",
      "listen_addr": "0.0.0.0:30303",
      "net_db": "ton-db",
      "use_public_overlays": true,
      "quic": false,
      "plumtree": false
    }
  },
  "account_states": { "enabled": true, "max_data_boc_size": 65536 },
  "pipeline": { "depth": 4, "block_fetch_parallelism": 16,
                "state_fetch_parallelism": 8, "parse_workers": 8 }
}
```

Startup semantics: with an existing database the node continues from the
committed progress; otherwise `from_seqno > 0` starts from that masterchain
seqno (liteservers required), and `from_seqno = 0` starts at the current
tip (liteserver `getMasterchainInfo`; in pure overlay mode at the global
config's `init_block`).

## Querying

`ton-librarian-query <db> <command>` prints JSON (all hashes/addresses are
hex, shard prefixes are hex, all numbers decimal):

```
status                                   — indexing progress
block <wc:shard:seqno>                   — block header info
txs-of-block <wc:shard:seqno>            — transaction hashes of a block
tx <hash>                                — full transaction with phases
account-txs <wc:addr-hex> [limit] [asc|desc]
msg <hash>                               — full message
msg-content <hash>                       — message body BoC (hex)
msg-txs <hash>                           — transactions linked to a message
account-msgs <wc:addr-hex> [limit] [asc|desc]
account <wc:addr-hex>                   — latest account state
account-history <wc:addr-hex> [limit]    — account state history
```

Example:

```bash
./build/src/ton-librarian-query librarian-db status
./build/src/ton-librarian-query librarian-db block -1:8000000000000000:43991050
./build/src/ton-librarian-query librarian-db tx 6c02...d1
```

## Storage layout (RocksDB column families)

- `blocks` — block headers (`wc:shard:seqno` → record)
- `block_txs` — block → transaction hashes
- `txs` — transaction hash → full record (incl. phases)
- `tx_by_account` — `(account, lt)` → tx hash
- `messages` — message hash → full record
- `msg_by_account` — `(account, dir, lt)` → message hash
- `msg_txs` — `(message hash, dir, idx)` → tx hash
- `msg_content` — message hash → body BoC
- `accounts` — account → latest state
- `account_states` — `(account, mc_seqno)` → state record
- `shard_states` — mc seqno → shard tops of the slice
- `progress` — global progress/counters

## License

LGPL v2.1-or-later, like ton itself.
