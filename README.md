# Trade Indexer

This indexer watches DEX trade events and sends them to Redis streams `trade_pool` (one event for each trade action, so 1 transaction can have multiple of these), `trade_swap` (in this case, multi-step swaps are stored as 1 event, also contains net balance changes for each asset), and `trade_pool_change` (changes in the pool, triggered by swaps, LP events, pool edit events, and more. If you want the most precise price change events, you should watch for this one).

To run it, set `REDIS_URL` environment variable and `cargo run --release`.
