use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::BlockHeight, neardata_old::OldNeardataProvider, BlockRange,
};
use intear_events::events::trade::trade_pool_change::AidolsPool;
use std::collections::HashMap;

use inindexer::{
    near_indexer_primitives::types::AccountId, run_indexer, IndexerOptions,
    PreprocessTransactionsSettings,
};

use crate::{
    ref_finance_state, BalanceChangeSwap, PoolChangeEvent, PoolId, PoolType, RawPoolSwap,
    TradeContext, TradeEventHandler, TradeIndexer,
};

#[derive(Default)]
struct TestHandler {
    pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
    balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    state_changes: Vec<PoolChangeEvent>,
    liquidity_pool_events: Vec<(TradeContext, PoolId, HashMap<AccountId, i128>)>,
}

#[async_trait]
impl TradeEventHandler for TestHandler {
    async fn on_raw_pool_swap(&mut self, context: TradeContext, swap: RawPoolSwap) {
        self.pool_swaps
            .entry(context.trader.clone())
            .or_default()
            .push((swap, context));
    }

    async fn on_balance_change_swap(
        &mut self,
        context: TradeContext,
        balance_changes: BalanceChangeSwap,
    ) {
        self.balance_change_swaps
            .entry(context.trader.clone())
            .or_default()
            .push((balance_changes, context));
    }

    async fn on_pool_change(&mut self, pool: PoolChangeEvent) {
        self.state_changes.push(pool);
    }

    async fn on_liquidity_pool(
        &mut self,
        context: TradeContext,
        pool_id: PoolId,
        tokens: HashMap<AccountId, i128>,
    ) {
        self.liquidity_pool_events.push((context, pool_id, tokens));
    }

    async fn flush_events(&mut self, _block_height: BlockHeight) {
        // No-op for test handler
    }
}

#[tokio::test]
async fn detects_ref_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 118_210_089,
                end_exclusive: Some(118_210_094),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"skyto.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "REF-5059".to_owned(),
                token_in: "wrap.near".parse().unwrap(),
                token_out: "meek.tkn.near".parse().unwrap(),
                amount_in: 1000000000000000000000000,
                amount_out: 93815865650297411273703890521643
            },
            TradeContext {
                trader: "skyto.near".parse().unwrap(),
                block_height: 118210091,
                block_timestamp_nanosec: 1714804406674985128,
                transaction_id: "E4okfxk1x6GdXA5YAwZpzyAqBnnXfo5XfKxj6cMF62Ky"
                    .parse()
                    .unwrap(),
                receipt_id: "VPrcZiwgFqKgW9eev4CUKJ4TN8Jk1jSZ2sqFAHothnN"
                    .parse()
                    .unwrap(),
            }
        )]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"skyto.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), -1000000000000000000000000),
                    (
                        "meek.tkn.near".parse().unwrap(),
                        93815865650297411273703890521643
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "REF-5059".to_owned(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "meek.tkn.near".parse().unwrap(),
                    amount_in: 1000000000000000000000000,
                    amount_out: 93815865650297411273703890521643
                }]
            },
            TradeContext {
                trader: "skyto.near".parse().unwrap(),
                block_height: 118210091,
                block_timestamp_nanosec: 1714804406674985128,
                transaction_id: "E4okfxk1x6GdXA5YAwZpzyAqBnnXfo5XfKxj6cMF62Ky"
                    .parse()
                    .unwrap(),
                receipt_id: "VPrcZiwgFqKgW9eev4CUKJ4TN8Jk1jSZ2sqFAHothnN"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_multistep_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 118_214_454,
                end_exclusive: Some(118_214_461),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"williamxx.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (
                RawPoolSwap {
                    pool: "REF-4663".to_owned(),
                    token_in: "intel.tkn.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 137002618695271800286520468,
                    amount_out: 26780878168917710181181086
                },
                TradeContext {
                    trader: "williamxx.near".parse().unwrap(),
                    block_height: 118214456,
                    block_timestamp_nanosec: 1714810103667818241,
                    transaction_id: "HQs1nW3B7XAc6RT7vP6vmmp2YRz19pY1avf6rWQpby3a"
                        .parse()
                        .unwrap(),
                    receipt_id: "8Ux6ezDRgMAXsVtKysjhz7vvWSGrg5Fc2bYLeFVZACK"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4921".to_owned(),
                    token_in: "intel.tkn.near".parse().unwrap(),
                    token_out: "wojak.tkn.near".parse().unwrap(),
                    amount_in: 3527689591892726209943536,
                    amount_out: 134692454322063117313149
                },
                TradeContext {
                    trader: "williamxx.near".parse().unwrap(),
                    block_height: 118214456,
                    block_timestamp_nanosec: 1714810103667818241,
                    transaction_id: "HQs1nW3B7XAc6RT7vP6vmmp2YRz19pY1avf6rWQpby3a"
                        .parse()
                        .unwrap(),
                    receipt_id: "8Ux6ezDRgMAXsVtKysjhz7vvWSGrg5Fc2bYLeFVZACK"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4875".to_owned(),
                    token_in: "wojak.tkn.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 134692454322063117313149,
                    amount_out: 689165024382991682878108
                },
                TradeContext {
                    trader: "williamxx.near".parse().unwrap(),
                    block_height: 118214456,
                    block_timestamp_nanosec: 1714810103667818241,
                    transaction_id: "HQs1nW3B7XAc6RT7vP6vmmp2YRz19pY1avf6rWQpby3a"
                        .parse()
                        .unwrap(),
                    receipt_id: "8Ux6ezDRgMAXsVtKysjhz7vvWSGrg5Fc2bYLeFVZACK"
                        .parse()
                        .unwrap(),
                }
            )
        ]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"williamxx.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), 27470043193300701864059194),
                    (
                        "intel.tkn.near".parse().unwrap(),
                        -140530308287164526496464004
                    )
                ]),
                pool_swaps: vec![
                    RawPoolSwap {
                        pool: "REF-4663".to_owned(),
                        token_in: "intel.tkn.near".parse().unwrap(),
                        token_out: "wrap.near".parse().unwrap(),
                        amount_in: 137002618695271800286520468,
                        amount_out: 26780878168917710181181086
                    },
                    RawPoolSwap {
                        pool: "REF-4921".to_owned(),
                        token_in: "intel.tkn.near".parse().unwrap(),
                        token_out: "wojak.tkn.near".parse().unwrap(),
                        amount_in: 3527689591892726209943536,
                        amount_out: 134692454322063117313149
                    },
                    RawPoolSwap {
                        pool: "REF-4875".to_owned(),
                        token_in: "wojak.tkn.near".parse().unwrap(),
                        token_out: "wrap.near".parse().unwrap(),
                        amount_in: 134692454322063117313149,
                        amount_out: 689165024382991682878108
                    }
                ]
            },
            TradeContext {
                trader: "williamxx.near".parse().unwrap(),
                block_height: 118214456,
                block_timestamp_nanosec: 1714810103667818241,
                transaction_id: "HQs1nW3B7XAc6RT7vP6vmmp2YRz19pY1avf6rWQpby3a"
                    .parse()
                    .unwrap(),
                receipt_id: "8Ux6ezDRgMAXsVtKysjhz7vvWSGrg5Fc2bYLeFVZACK"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_dragonbot_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 118_209_234,
                end_exclusive: Some(118_209_239),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(
                &"kxf05k08ps1ol3zgcwvmkam_dragon.dragon_bot.near"
                    .parse::<AccountId>()
                    .unwrap()
            )
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "REF-5059".to_owned(),
                token_in: "meek.tkn.near".parse().unwrap(),
                token_out: "wrap.near".parse().unwrap(),
                amount_in: 478481220062017777819333235161697,
                amount_out: 9466638646302120499119272
            },
            TradeContext {
                trader: "kxf05k08ps1ol3zgcwvmkam_dragon.dragon_bot.near"
                    .parse()
                    .unwrap(),
                block_height: 118209236,
                block_timestamp_nanosec: 1714803352814919506,
                transaction_id: "C4pr5yYyxviWQkt4K7uVFaH14LWR43gcKpj1GDiV4nc8"
                    .parse()
                    .unwrap(),
                receipt_id: "4xmgsfQ6YypjKC2hxts11YBuRNYjaavShtrpRAWxFHNu"
                    .parse()
                    .unwrap(),
            }
        )]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(
                &"kxf05k08ps1ol3zgcwvmkam_dragon.dragon_bot.near"
                    .parse::<AccountId>()
                    .unwrap()
            )
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), 9466638646302120499119272),
                    (
                        "meek.tkn.near".parse().unwrap(),
                        -478481220062017777819333235161697
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "REF-5059".to_owned(),
                    token_in: "meek.tkn.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 478481220062017777819333235161697,
                    amount_out: 9466638646302120499119272
                }]
            },
            TradeContext {
                trader: "kxf05k08ps1ol3zgcwvmkam_dragon.dragon_bot.near"
                    .parse()
                    .unwrap(),
                block_height: 118209236,
                block_timestamp_nanosec: 1714803352814919506,
                transaction_id: "C4pr5yYyxviWQkt4K7uVFaH14LWR43gcKpj1GDiV4nc8"
                    .parse()
                    .unwrap(),
                receipt_id: "4xmgsfQ6YypjKC2hxts11YBuRNYjaavShtrpRAWxFHNu"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_arbitrage_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 118_212_504,
                end_exclusive: Some(118_212_506),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"bot.marior.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (
                RawPoolSwap {
                    pool: "REF-4369".to_owned(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "token.0xshitzu.near".parse().unwrap(),
                    amount_in: 520000000000000000000000,
                    amount_out: 3244576408763446222268
                },
                TradeContext {
                    trader: "bot.marior.near".parse().unwrap(),
                    block_height: 118212505,
                    block_timestamp_nanosec: 1714807557910817723,
                    transaction_id: "8GxZPccqVMhXmrU1kZMJ1fSrnZ28kaPipiYQRPNT43BG"
                        .parse()
                        .unwrap(),
                    receipt_id: "FGYgTGuWkJD6W7wFXmFkP95rxdGbmxPWbNLTttFEwUam"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4821".to_owned(),
                    token_in: "token.0xshitzu.near".parse().unwrap(),
                    token_out: "nkok.tkn.near".parse().unwrap(),
                    amount_in: 3244576408763446222268,
                    amount_out: 11186538717588640655335259
                },
                TradeContext {
                    trader: "bot.marior.near".parse().unwrap(),
                    block_height: 118212505,
                    block_timestamp_nanosec: 1714807557910817723,
                    transaction_id: "8GxZPccqVMhXmrU1kZMJ1fSrnZ28kaPipiYQRPNT43BG"
                        .parse()
                        .unwrap(),
                    receipt_id: "FGYgTGuWkJD6W7wFXmFkP95rxdGbmxPWbNLTttFEwUam"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4913".to_owned(),
                    token_in: "nkok.tkn.near".parse().unwrap(),
                    token_out: "slush.tkn.near".parse().unwrap(),
                    amount_in: 11186538717588640655335259,
                    amount_out: 88180050805911386368580
                },
                TradeContext {
                    trader: "bot.marior.near".parse().unwrap(),
                    block_height: 118212505,
                    block_timestamp_nanosec: 1714807557910817723,
                    transaction_id: "8GxZPccqVMhXmrU1kZMJ1fSrnZ28kaPipiYQRPNT43BG"
                        .parse()
                        .unwrap(),
                    receipt_id: "FGYgTGuWkJD6W7wFXmFkP95rxdGbmxPWbNLTttFEwUam"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4911".to_owned(),
                    token_in: "slush.tkn.near".parse().unwrap(),
                    token_out: "wojak.tkn.near".parse().unwrap(),
                    amount_in: 88180050805911386368580,
                    amount_out: 102552548670451059547623
                },
                TradeContext {
                    trader: "bot.marior.near".parse().unwrap(),
                    block_height: 118212505,
                    block_timestamp_nanosec: 1714807557910817723,
                    transaction_id: "8GxZPccqVMhXmrU1kZMJ1fSrnZ28kaPipiYQRPNT43BG"
                        .parse()
                        .unwrap(),
                    receipt_id: "FGYgTGuWkJD6W7wFXmFkP95rxdGbmxPWbNLTttFEwUam"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4875".to_owned(),
                    token_in: "wojak.tkn.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 102552548670451059547623,
                    amount_out: 525408551701397302192601
                },
                TradeContext {
                    trader: "bot.marior.near".parse().unwrap(),
                    block_height: 118212505,
                    block_timestamp_nanosec: 1714807557910817723,
                    transaction_id: "8GxZPccqVMhXmrU1kZMJ1fSrnZ28kaPipiYQRPNT43BG"
                        .parse()
                        .unwrap(),
                    receipt_id: "FGYgTGuWkJD6W7wFXmFkP95rxdGbmxPWbNLTttFEwUam"
                        .parse()
                        .unwrap(),
                }
            )
        ]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"bot.marior.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([(
                    "wrap.near".parse().unwrap(),
                    5408551701397302192601
                )]),
                pool_swaps: vec![
                    RawPoolSwap {
                        pool: "REF-4369".to_owned(),
                        token_in: "wrap.near".parse().unwrap(),
                        token_out: "token.0xshitzu.near".parse().unwrap(),
                        amount_in: 520000000000000000000000,
                        amount_out: 3244576408763446222268
                    },
                    RawPoolSwap {
                        pool: "REF-4821".to_owned(),
                        token_in: "token.0xshitzu.near".parse().unwrap(),
                        token_out: "nkok.tkn.near".parse().unwrap(),
                        amount_in: 3244576408763446222268,
                        amount_out: 11186538717588640655335259
                    },
                    RawPoolSwap {
                        pool: "REF-4913".to_owned(),
                        token_in: "nkok.tkn.near".parse().unwrap(),
                        token_out: "slush.tkn.near".parse().unwrap(),
                        amount_in: 11186538717588640655335259,
                        amount_out: 88180050805911386368580
                    },
                    RawPoolSwap {
                        pool: "REF-4911".to_owned(),
                        token_in: "slush.tkn.near".parse().unwrap(),
                        token_out: "wojak.tkn.near".parse().unwrap(),
                        amount_in: 88180050805911386368580,
                        amount_out: 102552548670451059547623
                    },
                    RawPoolSwap {
                        pool: "REF-4875".to_owned(),
                        token_in: "wojak.tkn.near".parse().unwrap(),
                        token_out: "wrap.near".parse().unwrap(),
                        amount_in: 102552548670451059547623,
                        amount_out: 525408551701397302192601
                    }
                ]
            },
            TradeContext {
                trader: "bot.marior.near".parse().unwrap(),
                block_height: 118212505,
                block_timestamp_nanosec: 1714807557910817723,
                transaction_id: "8GxZPccqVMhXmrU1kZMJ1fSrnZ28kaPipiYQRPNT43BG"
                    .parse()
                    .unwrap(),
                receipt_id: "FGYgTGuWkJD6W7wFXmFkP95rxdGbmxPWbNLTttFEwUam"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn doesnt_detect_failed_ref_arbitrage_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 118_214_071,
                end_exclusive: Some(118_214_073),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .handler
            .pool_swaps
            .get(&"bot.marior.near".parse::<AccountId>().unwrap()),
        None
    );
    assert_eq!(
        indexer
            .handler
            .balance_change_swaps
            .get(&"bot.marior.near".parse::<AccountId>().unwrap()),
        None
    );
}

#[tokio::test]
async fn doesnt_detect_failed_ref_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 112_087_639,
                end_exclusive: Some(112_087_643),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .handler
            .pool_swaps
            .get(&"slimegirl.near".parse::<AccountId>().unwrap()),
        None
    );
    assert_eq!(
        indexer
            .handler
            .balance_change_swaps
            .get(&"slimegirl.near".parse::<AccountId>().unwrap()),
        None
    );
}

#[tokio::test]
async fn detects_delegate_ref_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 115_224_414,
                end_exclusive: Some(115_224_420),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"alanmain.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (
                RawPoolSwap {
                    pool: "REF-3879".to_owned(),
                    token_in: "usdt.tether-token.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 29992989,
                    amount_out: 4403363405586660846534469
                },
                TradeContext {
                    trader: "alanmain.near".parse().unwrap(),
                    block_height: 115224417,
                    block_timestamp_nanosec: 1711109366547729030,
                    transaction_id: "AM6t5vuuShi8qFjunBzvWbqCo9rh9Ttk4XzJnPXAvGsk"
                        .parse()
                        .unwrap(),
                    receipt_id: "2rb7u5GeRdDLnyM9ggKg4RMBge3UMCbuwk5Gr9fC5jon"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4663".to_owned(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "intel.tkn.near".parse().unwrap(),
                    amount_in: 4403363405586660846534469,
                    amount_out: 43884510175556511587239906
                },
                TradeContext {
                    trader: "alanmain.near".parse().unwrap(),
                    block_height: 115224417,
                    block_timestamp_nanosec: 1711109366547729030,
                    transaction_id: "AM6t5vuuShi8qFjunBzvWbqCo9rh9Ttk4XzJnPXAvGsk"
                        .parse()
                        .unwrap(),
                    receipt_id: "2rb7u5GeRdDLnyM9ggKg4RMBge3UMCbuwk5Gr9fC5jon"
                        .parse()
                        .unwrap(),
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-4668".to_owned(),
                    token_in: "usdt.tether-token.near".parse().unwrap(),
                    token_out: "intel.tkn.near".parse().unwrap(),
                    amount_in: 11647,
                    amount_out: 17258755648110183139126
                },
                TradeContext {
                    trader: "alanmain.near".parse().unwrap(),
                    block_height: 115224417,
                    block_timestamp_nanosec: 1711109366547729030,
                    transaction_id: "AM6t5vuuShi8qFjunBzvWbqCo9rh9Ttk4XzJnPXAvGsk"
                        .parse()
                        .unwrap(),
                    receipt_id: "2rb7u5GeRdDLnyM9ggKg4RMBge3UMCbuwk5Gr9fC5jon"
                        .parse()
                        .unwrap(),
                }
            )
        ]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"alanmain.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    (
                        "intel.tkn.near".parse().unwrap(),
                        43901768931204621770379032
                    ),
                    ("usdt.tether-token.near".parse().unwrap(), -30004636)
                ]),
                pool_swaps: vec![
                    RawPoolSwap {
                        pool: "REF-3879".to_owned(),
                        token_in: "usdt.tether-token.near".parse().unwrap(),
                        token_out: "wrap.near".parse().unwrap(),
                        amount_in: 29992989,
                        amount_out: 4403363405586660846534469
                    },
                    RawPoolSwap {
                        pool: "REF-4663".to_owned(),
                        token_in: "wrap.near".parse().unwrap(),
                        token_out: "intel.tkn.near".parse().unwrap(),
                        amount_in: 4403363405586660846534469,
                        amount_out: 43884510175556511587239906
                    },
                    RawPoolSwap {
                        pool: "REF-4668".to_owned(),
                        token_in: "usdt.tether-token.near".parse().unwrap(),
                        token_out: "intel.tkn.near".parse().unwrap(),
                        amount_in: 11647,
                        amount_out: 17258755648110183139126
                    }
                ]
            },
            TradeContext {
                trader: "alanmain.near".parse().unwrap(),
                block_height: 115224417,
                block_timestamp_nanosec: 1711109366547729030,
                transaction_id: "AM6t5vuuShi8qFjunBzvWbqCo9rh9Ttk4XzJnPXAvGsk"
                    .parse()
                    .unwrap(),
                receipt_id: "2rb7u5GeRdDLnyM9ggKg4RMBge3UMCbuwk5Gr9fC5jon"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_state_changes() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 118_210_089,
                end_exclusive: Some(118_210_094),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer.handler.state_changes,
        vec![PoolChangeEvent {
            pool_id: "REF-5059".to_owned(),
            receipt_id: "VPrcZiwgFqKgW9eev4CUKJ4TN8Jk1jSZ2sqFAHothnN"
                .parse()
                .unwrap(),
            block_height: 118210091,
            block_timestamp_nanosec: 1714804406674985128,
            pool: PoolType::Ref(ref_finance_state::Pool::SimplePool(
                ref_finance_state::SimplePool {
                    token_account_ids: vec![
                        "meek.tkn.near".parse().unwrap(),
                        "wrap.near".parse().unwrap()
                    ],
                    amounts: vec![828179771760105311265410344967355, 9801232357889642407258332],
                    volumes: vec![
                        ref_finance_state::SwapVolume {
                            input: 9848609675470765100937508071657111,
                            output: 46120275647008127734385064
                        },
                        ref_finance_state::SwapVolume {
                            input: 52320628138265857406741776,
                            output: 14320429903710659789672097345488919
                        }
                    ],
                    total_fee: 30,
                    exchange_fee: 0,
                    referral_fee: 0,
                    shares_prefix: vec![2, 195, 19, 0, 0],
                    shares_total_supply: 1495131888301825452817183
                }
            ))
        }]
    );
}

#[tokio::test]
async fn detects_ref_hot_tg_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 124_427_306,
                end_exclusive: Some(124_427_323),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"acejapan.tg".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![
            (
                RawPoolSwap {
                    pool: "REF-5222".to_string(),
                    token_in: "dd.tg".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 933200000000,
                    amount_out: 1694993438147166311514743
                },
                TradeContext {
                    trader: "acejapan.tg".parse().unwrap(),
                    block_height: 124427317,
                    block_timestamp_nanosec: 1722139552074832400,
                    transaction_id: "BJJiADeRfDhgvTNbmyJz3Xj1P86iQmX9791RXo33KxCN"
                        .parse()
                        .unwrap(),
                    receipt_id: "4wVWyZd2k1vbSQCw4HzvvKVqrgsUYRiEoiRDQUtYX5Yu"
                        .parse()
                        .unwrap()
                }
            ),
            (
                RawPoolSwap {
                    pool: "REF-3879".to_string(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "usdt.tether-token.near".parse().unwrap(),
                    amount_in: 1694993438147166311514743,
                    amount_out: 9458256
                },
                TradeContext {
                    trader: "acejapan.tg".parse().unwrap(),
                    block_height: 124427317,
                    block_timestamp_nanosec: 1722139552074832400,
                    transaction_id: "BJJiADeRfDhgvTNbmyJz3Xj1P86iQmX9791RXo33KxCN"
                        .parse()
                        .unwrap(),
                    receipt_id: "4wVWyZd2k1vbSQCw4HzvvKVqrgsUYRiEoiRDQUtYX5Yu"
                        .parse()
                        .unwrap()
                }
            )
        ]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"acejapan.tg".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("usdt.tether-token.near".parse().unwrap(), 9458256),
                    ("dd.tg".parse().unwrap(), -933200000000),
                ]),
                pool_swaps: vec![
                    RawPoolSwap {
                        pool: "REF-5222".to_string(),
                        token_in: "dd.tg".parse().unwrap(),
                        token_out: "wrap.near".parse().unwrap(),
                        amount_in: 933200000000,
                        amount_out: 1694993438147166311514743
                    },
                    RawPoolSwap {
                        pool: "REF-3879".to_string(),
                        token_in: "wrap.near".parse().unwrap(),
                        token_out: "usdt.tether-token.near".parse().unwrap(),
                        amount_in: 1694993438147166311514743,
                        amount_out: 9458256
                    }
                ]
            },
            TradeContext {
                trader: "acejapan.tg".parse().unwrap(),
                block_height: 124427317,
                block_timestamp_nanosec: 1722139552074832400,
                transaction_id: "BJJiADeRfDhgvTNbmyJz3Xj1P86iQmX9791RXo33KxCN"
                    .parse()
                    .unwrap(),
                receipt_id: "4wVWyZd2k1vbSQCw4HzvvKVqrgsUYRiEoiRDQUtYX5Yu"
                    .parse()
                    .unwrap()
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_liquidity_add() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 129_352_974,
                end_exclusive: Some(129_352_978),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer.handler.liquidity_pool_events,
        vec![(
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 129352975,
                block_timestamp_nanosec: 1727829382059005601,
                transaction_id: "HyaTXZkaEDhPouF3L2AfmE4Pg8epP2kzX2d4jxgvnknE"
                    .parse()
                    .unwrap(),
                receipt_id: "GFU7m8uKS7unATiG6KSPjqa2zBjH1BaVoJMSQrR2rkF6"
                    .parse()
                    .unwrap(),
            },
            "REF-4663".to_owned(),
            HashMap::from_iter([
                ("wrap.near".parse().unwrap(), 999999999999999915648607),
                (
                    "intel.tkn.near".parse().unwrap(),
                    15869989324782287999975226
                )
            ])
        )]
    );
}

#[tokio::test]
async fn detects_ref_liquidity_remove() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 129_364_250,
                end_exclusive: Some(129_364_254),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer.handler.liquidity_pool_events,
        vec![(
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 129364252,
                block_timestamp_nanosec: 1727842012958701333,
                transaction_id: "7B124NAr1MktLjGbjiYFPBP1guXSkgp5TzAJvFzmX4xb"
                    .parse()
                    .unwrap(),
                receipt_id: "89gwSxyXaWDABkjgRSpRTKVEced9RpCX2UT8uXR5FsJR"
                    .parse()
                    .unwrap(),
            },
            "REF-4663".to_owned(),
            HashMap::from_iter([
                ("wrap.near".parse().unwrap(), -1000312838374558764552331),
                (
                    "intel.tkn.near".parse().unwrap(),
                    -15865198314126424586378752
                )
            ])
        )]
    );
}

#[tokio::test]
async fn detects_ref_swap_by_output() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 131_092_276,
                end_exclusive: Some(131_092_280),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"fiery_drone.user.intear.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "REF-4663".to_owned(),
                token_in: "wrap.near".parse().unwrap(),
                token_out: "intel.tkn.near".parse().unwrap(),
                amount_in: 706788683547272399546037,
                amount_out: 14932514982037617660395520
            },
            TradeContext {
                trader: "fiery_drone.user.intear.near".parse().unwrap(),
                block_height: 131092278,
                block_timestamp_nanosec: 1729777813518885252,
                transaction_id: "39rFvuHaD7BXgteZHjPxkzxPmXN7ffmhhP3NKn6EjHoj"
                    .parse()
                    .unwrap(),
                receipt_id: "AeUZ7w79WAFjoJkAKogWWU8HSPo9rwjY6yhyjumM7Md5"
                    .parse()
                    .unwrap(),
            }
        )]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"fiery_drone.user.intear.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), -706788683547272399546037),
                    (
                        "intel.tkn.near".parse().unwrap(),
                        14932514982037617660395520,
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "REF-4663".to_owned(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "intel.tkn.near".parse().unwrap(),
                    amount_in: 706788683547272399546037,
                    amount_out: 14932514982037617660395520
                },]
            },
            TradeContext {
                trader: "fiery_drone.user.intear.near".parse().unwrap(),
                block_height: 131092278,
                block_timestamp_nanosec: 1729777813518885252,
                transaction_id: "39rFvuHaD7BXgteZHjPxkzxPmXN7ffmhhP3NKn6EjHoj"
                    .parse()
                    .unwrap(),
                receipt_id: "AeUZ7w79WAFjoJkAKogWWU8HSPo9rwjY6yhyjumM7Md5"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_swap_by_output_transfer() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 142_760_523,
                end_exclusive: Some(142_760_532),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(
                &"d0ebc7d872d5e3ee9281e9492aa5aca606cbc829c7dfc915a168ac75ccc23e7e"
                    .parse::<AccountId>()
                    .unwrap()
            )
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "REF-6031".to_owned(),
                token_in: "end.aidols.near".parse().unwrap(),
                token_out: "wrap.near".parse().unwrap(),
                amount_in: 3696035670585457669556649429,
                amount_out: 78838174273858921161827
            },
            TradeContext {
                trader: "d0ebc7d872d5e3ee9281e9492aa5aca606cbc829c7dfc915a168ac75ccc23e7e"
                    .parse()
                    .unwrap(),
                block_height: 142760528,
                block_timestamp_nanosec: 1743008136820312282,
                transaction_id: "FeEQwTYHWY5iHBUELM7DDBrmoNNaZzWztvmYjXB5cCDD"
                    .parse()
                    .unwrap(),
                receipt_id: "8hPEQfwhxU1zt1grxiLHysTb5fwk6VJMEC17cnA5oLRZ"
                    .parse()
                    .unwrap(),
            }
        )]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(
                &"d0ebc7d872d5e3ee9281e9492aa5aca606cbc829c7dfc915a168ac75ccc23e7e"
                    .parse::<AccountId>()
                    .unwrap()
            )
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), 78838174273858921161827),
                    (
                        "end.aidols.near".parse().unwrap(),
                        -3696035670585457669556649429,
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "REF-6031".to_owned(),
                    token_in: "end.aidols.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 3696035670585457669556649429,
                    amount_out: 78838174273858921161827
                },]
            },
            TradeContext {
                trader: "d0ebc7d872d5e3ee9281e9492aa5aca606cbc829c7dfc915a168ac75ccc23e7e"
                    .parse()
                    .unwrap(),
                block_height: 142760528,
                block_timestamp_nanosec: 1743008136820312282,
                transaction_id: "FeEQwTYHWY5iHBUELM7DDBrmoNNaZzWztvmYjXB5cCDD"
                    .parse()
                    .unwrap(),
                receipt_id: "8hPEQfwhxU1zt1grxiLHysTb5fwk6VJMEC17cnA5oLRZ"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_aidols_buy() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 137406119,
                end_exclusive: Some(137406124),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "AIDOLS-ponkeai.aidols.near".to_owned(),
                token_in: "wrap.near".parse().unwrap(),
                token_out: "ponkeai.aidols.near".parse().unwrap(),
                amount_in: 300000000000000000000000,
                amount_out: 399840063974410235905637744903
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 137406122,
                block_timestamp_nanosec: 1736934912940183334,
                transaction_id: "6xNcuGFB3Qs5hmDkavireqsxaENLGeJVw5St8PeXYnDz"
                    .parse()
                    .unwrap(),
                receipt_id: "3KiybrbFAbDMxcTYDmZpjBrQX7pKLGoMreoHpLa6kEWs"
                    .parse()
                    .unwrap(),
            }
        )]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), -300000000000000000000000),
                    (
                        "ponkeai.aidols.near".parse().unwrap(),
                        399840063974410235905637744903,
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "AIDOLS-ponkeai.aidols.near".to_owned(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "ponkeai.aidols.near".parse().unwrap(),
                    amount_in: 300000000000000000000000,
                    amount_out: 399840063974410235905637744903
                }]
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 137406122,
                block_timestamp_nanosec: 1736934912940183334,
                transaction_id: "6xNcuGFB3Qs5hmDkavireqsxaENLGeJVw5St8PeXYnDz"
                    .parse()
                    .unwrap(),
                receipt_id: "3KiybrbFAbDMxcTYDmZpjBrQX7pKLGoMreoHpLa6kEWs"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_aidols_sell() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 137409038,
                end_exclusive: Some(137409042),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "AIDOLS-ponkeai.aidols.near".to_owned(),
                token_in: "ponkeai.aidols.near".parse().unwrap(),
                token_out: "wrap.near".parse().unwrap(),
                amount_in: 399840063974410235905637744903,
                amount_out: 100000000000000000000001
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 137409041,
                block_timestamp_nanosec: 1736938235180073028,
                transaction_id: "HcQJKrS9UHgqvJjMAyJSJvP8odkdky3tdR82mMjnrV6K"
                    .parse()
                    .unwrap(),
                receipt_id: "C7HHJztaC9ngMqMurUJQbbAb3HwtVJSuKcAjrPMM71yd"
                    .parse()
                    .unwrap(),
            }
        )]
    );

    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), 100000000000000000000001),
                    (
                        "ponkeai.aidols.near".parse().unwrap(),
                        -399840063974410235905637744903
                    ),
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "AIDOLS-ponkeai.aidols.near".to_owned(),
                    token_in: "ponkeai.aidols.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 399840063974410235905637744903,
                    amount_out: 100000000000000000000001
                }],
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 137409041,
                block_timestamp_nanosec: 1736938235180073028,
                transaction_id: "HcQJKrS9UHgqvJjMAyJSJvP8odkdky3tdR82mMjnrV6K"
                    .parse()
                    .unwrap(),
                receipt_id: "C7HHJztaC9ngMqMurUJQbbAb3HwtVJSuKcAjrPMM71yd"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_aidols_state_changes() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 137406979,
                end_exclusive: Some(137406984),
            })
        },
    )
    .await
    .unwrap();

    assert!(indexer.handler.state_changes.contains(&PoolChangeEvent {
        pool_id: "AIDOLS-tganza.aidols.near".to_owned(),
        receipt_id: "ErBeAEQyuWyab7ggYrzEZnPBo1sJA4GnJ6PhiCrMnn9y"
            .parse()
            .unwrap(),
        block_timestamp_nanosec: 1736935882233587330,
        block_height: 137406981,
        pool: PoolType::Aidols(AidolsPool {
            token_id: "tganza.aidols.near".parse().unwrap(),
            token_hold: 1000000000000000000000000000000000,
            wnear_hold: 500000000000000000000000000,
            is_deployed: false,
            is_tradable: true
        })
    }));
}

#[tokio::test]
async fn detects_refdcl_trades() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 143_270_323,
                end_exclusive: Some(143_270_329),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .handler
            .pool_swaps
            .get(&"5adcddad84c166d8792684c3ad652803df01fac582526dd5c21903b0b5aafe2d".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "REFDCL-17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1|wrap.near|100".to_owned(),
                token_in: "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1".parse().unwrap(),
                token_out: "wrap.near".parse().unwrap(),
                amount_in: 50287157,
                amount_out: 19802185927199304105095477
            },
            TradeContext {
                trader: "5adcddad84c166d8792684c3ad652803df01fac582526dd5c21903b0b5aafe2d".parse().unwrap(),
                block_height: 143270326,
                block_timestamp_nanosec: 1743580488884603339,
                transaction_id: "5SiQzAwvpfu3dBAao3TuaXhwLTFANDQ3GXNryR1aqdFk".parse().unwrap(),
                receipt_id: "8eznv1M9d33sPDHdUnzTCzduTxujuqG4kmUjU5tWJ3pk".parse().unwrap(),
            }
        )]
    );
    assert_eq!(
        *indexer
            .handler
            .balance_change_swaps
            .get(&"5adcddad84c166d8792684c3ad652803df01fac582526dd5c21903b0b5aafe2d".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            BalanceChangeSwap {
                balance_changes: HashMap::from_iter([
                    ("wrap.near".parse().unwrap(), 19802185927199304105095477),
                    (
                        "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1".parse().unwrap(),
                        -50287157
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "REFDCL-17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1|wrap.near|100".to_owned(),
                    token_in: "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 50287157,
                    amount_out: 19802185927199304105095477
                }]
            },
            TradeContext {
                trader: "5adcddad84c166d8792684c3ad652803df01fac582526dd5c21903b0b5aafe2d".parse().unwrap(),
                block_height: 143270326,
                block_timestamp_nanosec: 1743580488884603339,
                transaction_id: "5SiQzAwvpfu3dBAao3TuaXhwLTFANDQ3GXNryR1aqdFk".parse().unwrap(),
                receipt_id: "8eznv1M9d33sPDHdUnzTCzduTxujuqG4kmUjU5tWJ3pk".parse().unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_ref_degen_pool_state_changes() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        OldNeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 150_611_257,
                end_exclusive: Some(150_611_259),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer.handler.state_changes,
        vec![
            PoolChangeEvent {
                pool_id: "REF-5949".to_owned(),
                receipt_id: "FK1PA1PxUgPGuVTjkbAD6y2HUvpZLSHAmhJuXEHzHowN"
                    .parse()
                    .unwrap(),
                block_timestamp_nanosec: 1749623222162246603,
                block_height: 150611258,
                pool: PoolType::Ref(ref_finance_state::Pool::DegenSwapPool(
                    ref_finance_state::DegenSwapPool {
                        token_account_ids: vec![
                            "nbtc.bridge.near".parse().unwrap(),
                            "wrap.near".parse().unwrap()
                        ],
                        token_decimals: vec![8, 24],
                        c_amounts: vec![4642186285073671824501341, 350434995831534383783544203076],
                        volumes: vec![
                            ref_finance_state::SwapVolume {
                                input: 16929762,
                                output: 18260074
                            },
                            ref_finance_state::SwapVolume {
                                input: 5353689043801567558421196948,
                                output: 4948812684957635773598314254
                            }
                        ],
                        total_fee: 30,
                        shares_prefix: vec![2, 61, 23, 0, 0],
                        shares_total_supply: 1445246300131913021202509935799,
                        init_amp_factor: 60,
                        target_amp_factor: 60,
                        init_amp_time: 0,
                        stop_amp_time: 0
                    }
                ))
            },
            PoolChangeEvent {
                pool_id: "REF-5470".to_owned(),
                receipt_id: "GnytSH1oG2HiU3m7WFr6XUWMsVNkagi9hxvVGLMCxQG9"
                    .parse()
                    .unwrap(),
                block_timestamp_nanosec: 1749623222162246603,
                block_height: 150611258,
                pool: PoolType::Ref(ref_finance_state::Pool::SimplePool(
                    ref_finance_state::SimplePool {
                        token_account_ids: vec![
                            "wrap.near".parse().unwrap(),
                            "usdt.tether-token.near".parse().unwrap()
                        ],
                        amounts: vec![63747110087455234309348061106, 167818251621],
                        volumes: vec![
                            ref_finance_state::SwapVolume {
                                input: 10171840847556632003695494413256,
                                output: 49171401369268
                            },
                            ref_finance_state::SwapVolume {
                                input: 49152712420352,
                                output: 10161805147674006933064216166896
                            }
                        ],
                        total_fee: 1,
                        exchange_fee: 0,
                        referral_fee: 0,
                        shares_prefix: vec![2, 94, 21, 0, 0],
                        shares_total_supply: 502826573823564442482190
                    }
                ))
            }
        ]
    );
}
