# myHistory

A [Minima](https://minima.global) MiniDapp that reconstructs your node's full
transaction history from [block.minima.global](https://block.minima.global) and
keeps it locally **forever** — so it survives node re-syncs, which normally wipe
local history.

## Features

- Reconstructs history for your **default wallet** addresses, plus tracked
  **contract / script** addresses (run as a separate pass).
- Stores everything in the node's local H2 database — **searchable, sortable, paged**,
  with no size cap on the stored history.
- Per-transaction detail on row expand: **inputs / outputs / state**.
- **Export** the rows matching your current filters to **CSV** or **JSON**.
- New transactions are appended automatically after the first run.
- Optional coin-level **backfill from an archive node** for pre-explorer history.
- **Signature key-usage check** — counts how many times each signing key spent from your
  addresses and flags reused keys.
- Responsive UI, mobile → 4K.

## Install

Grab a build from [Releases](../../releases) and install it on your node:

```
mds action:install file:myhistory-0.2.2.mds.zip
```

## Build from source

```
zip -r myhistory.mds.zip dapp.conf index.html app.js mds.js icon.svg
```

## Versions

- **0.2.2** — adds a signature key-usage check (per-address key-use counts; flags reused signing keys).
- **0.2.1** — adds CSV / JSON export of the filtered history.
- **0.2.0** — summary history line; reconstructs default-wallet + contract/script
  addresses, fully SQL-backed search and paging.

## How it works

The dapp reads your node's tracked addresses (`mds action:scripts`), then queries
the explorer's `search.get` tRPC endpoint per address (proxied through the node via
`MDS.net.GET`, since the explorer sends no CORS headers). Results are classified by
direction, summarised, and upserted into a local `txns` table. Filtering, search,
sorting and pagination all run in SQL so memory stays bounded regardless of history size.
