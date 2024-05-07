use async_trait::async_trait;
use std::collections::HashMap;

use inindexer::{
    fastnear_data_server::FastNearDataServerProvider,
    near_indexer_primitives::types::{AccountId, BlockHeight},
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};

use crate::{
    ref_finance_state, BalanceChangeSwap, Pool, PoolId, PoolType, RawPoolSwap, TradeContext,
    TradeEventHandler, TradeIndexer,
};

#[tokio::test]
async fn detects_ref_trades() {
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_210_089..=118_210_094),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
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
            .0
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
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_214_454..=118_214_461),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
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
            .0
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
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_209_234..=118_209_239),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
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
            .0
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
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_212_504..=118_212_506),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
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
            .0
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
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_214_071..=118_214_073),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .pool_swaps
            .get(&"bot.marior.near".parse::<AccountId>().unwrap()),
        None
    );
    assert_eq!(
        indexer
            .0
            .balance_change_swaps
            .get(&"bot.marior.near".parse::<AccountId>().unwrap()),
        None
    );
}

#[tokio::test]
async fn doesnt_detect_failed_ref_trades() {
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(112_087_639..=112_087_643),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .pool_swaps
            .get(&"slimegirl.near".parse::<AccountId>().unwrap()),
        None
    );
    assert_eq!(
        indexer
            .0
            .balance_change_swaps
            .get(&"slimegirl.near".parse::<AccountId>().unwrap()),
        None
    );
}

#[tokio::test]
async fn detects_delegate_ref_trades() {
    struct TestHandler {
        pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
        balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, context: &TradeContext, swap: &RawPoolSwap) {
            self.pool_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((swap.clone(), context.clone()));
        }

        async fn on_balance_change_swap(
            &mut self,
            context: &TradeContext,
            balance_changes: &BalanceChangeSwap,
        ) {
            self.balance_change_swaps
                .entry(context.trader.clone())
                .or_default()
                .push((balance_changes.clone(), context.clone()));
        }

        async fn on_pool_change(&mut self, _pool: &Pool, _height: BlockHeight) {}
    }

    let handler = TestHandler {
        pool_swaps: HashMap::new(),
        balance_change_swaps: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(115_224_414..=115_224_420),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
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
            .0
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
    struct TestHandler {
        state_changes: HashMap<PoolId, Vec<Pool>>,
    }

    #[async_trait]
    impl TradeEventHandler for TestHandler {
        async fn on_raw_pool_swap(&mut self, _context: &TradeContext, _swap: &RawPoolSwap) {}

        async fn on_balance_change_swap(
            &mut self,
            _context: &TradeContext,
            _balance_changes: &BalanceChangeSwap,
        ) {
        }

        async fn on_pool_change(&mut self, pool: &Pool, _height: BlockHeight) {
            self.state_changes
                .entry(pool.id.clone())
                .or_default()
                .push(pool.clone());
        }
    }

    let handler = TestHandler {
        state_changes: HashMap::new(),
    };

    let mut indexer = TradeIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(118_210_089..=118_210_094),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer.0.state_changes.get("REF-5059").unwrap(),
        vec![Pool {
            id: "REF-5059".to_owned(),
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
