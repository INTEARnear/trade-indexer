use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::BlockHeight;
use intear_events::events::trade::trade_pool_change::{AidolsPool, GraFunPool};
use std::collections::HashMap;

use inindexer::{
    near_indexer_primitives::types::AccountId, neardata::NeardataProvider, run_indexer,
    BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};

use crate::meme_cooking_deposit_detection::{DepositEvent, WithdrawEvent};
use crate::{
    ref_finance_state, BalanceChangeSwap, PoolChangeEvent, PoolId, PoolType, RawPoolSwap,
    TradeContext, TradeEventHandler, TradeIndexer,
};

#[derive(Default)]
struct TestHandler {
    pool_swaps: HashMap<AccountId, Vec<(RawPoolSwap, TradeContext)>>,
    balance_change_swaps: HashMap<AccountId, Vec<(BalanceChangeSwap, TradeContext)>>,
    state_changes: Vec<PoolChangeEvent>,
    memecooking_deposits: Vec<(DepositEvent, TradeContext)>,
    memecooking_withdraws: Vec<(WithdrawEvent, TradeContext)>,
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

    async fn on_memecooking_deposit(&mut self, context: TradeContext, deposit: DepositEvent) {
        self.memecooking_deposits.push((deposit, context));
    }

    async fn on_memecooking_withdraw(&mut self, context: TradeContext, withdraw: WithdrawEvent) {
        self.memecooking_withdraws.push((withdraw, context));
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
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
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124_427_306..=124_427_323),
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
async fn detects_memecooking_deposits() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: true,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::testnet(),
        IndexerOptions {
            range: BlockIterator::iterator(174_733_296..=174_733_302),
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
        *indexer.handler.memecooking_deposits,
        vec![(
            DepositEvent {
                meme_id: 52,
                account_id: "slime.testnet".parse().unwrap(),
                amount: 2985000000000000000000000,
                protocol_fee: 7500000000000000000000,
                referrer: Some(
                    "0xd51c5283b8727206bf9be2b2db4e5673efaf519c"
                        .parse()
                        .unwrap()
                ),
                referrer_fee: Some(7500000000000000000000)
            },
            TradeContext {
                trader: "slime.testnet".parse().unwrap(),
                block_height: 174733299,
                block_timestamp_nanosec: 1726822053211742048,
                transaction_id: "3JKqU16HucfRagV5gNEtjfkZFwV5xZMwiTa2pYVt7oxa"
                    .parse()
                    .unwrap(),
                receipt_id: "2acCdtPJUkp37aW6jT66hedowjczzycVB5YKHfA2gnjg"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_memecooking_withdraws() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: true,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::testnet(),
        IndexerOptions {
            range: BlockIterator::iterator(174_938_562..=174_938_567),
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
        *indexer.handler.memecooking_withdraws,
        vec![(
            WithdrawEvent {
                meme_id: 53,
                account_id: "slime.testnet".parse().unwrap(),
                amount: 975100000000000000000000,
                fee: 19900000000000000000000,
            },
            TradeContext {
                trader: "slime.testnet".parse().unwrap(),
                block_height: 174938564,
                block_timestamp_nanosec: 1727027550926094610,
                transaction_id: "FGf3e9QDEBLYGCA11K3z4QaeoZtBxDNrUys1iErgBMaQ"
                    .parse()
                    .unwrap(),
                receipt_id: "G6k8gYVVNAyf9XZC6H8Xby6mLx7SztAq8tgBLAUMK7e2"
                    .parse()
                    .unwrap(),
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
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(129_352_974..=129_352_978),
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
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(129_364_250..=129_364_254),
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
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(131_092_276..=131_092_280),
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
async fn detects_aidols_buy() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(137406119..=137406124),
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
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(137409038..=137409042),
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
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(137406979..=137406984),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        dbg!(indexer.handler.state_changes).contains(&PoolChangeEvent {
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
        })
    );
}

#[tokio::test]
async fn detects_grafun_buy() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(139464959..=139464969),
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
            .handler
            .pool_swaps
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "GRAFUN-worm.gra-fun.near".to_owned(),
                token_in: "wrap.near".parse().unwrap(),
                token_out: "worm.gra-fun.near".parse().unwrap(),
                amount_in: 1000000000000000000000000,
                amount_out: 1189351822990900207532202097059
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 139464963,
                block_timestamp_nanosec: 1739256255986699436,
                transaction_id: "CWm8Y6XgGdR2GHDJkzFkZHuG1uwicXEfbKpH41LWrB8E"
                    .parse()
                    .unwrap(),
                receipt_id: "3cN6x6opaVxnUBSGg8tR3oT2RFHTus3Y56p2oMKhLZ9D"
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
                    ("wrap.near".parse().unwrap(), -1000000000000000000000000),
                    (
                        "worm.gra-fun.near".parse().unwrap(),
                        1189351822990900207532202097059,
                    )
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "GRAFUN-worm.gra-fun.near".to_owned(),
                    token_in: "wrap.near".parse().unwrap(),
                    token_out: "worm.gra-fun.near".parse().unwrap(),
                    amount_in: 1000000000000000000000000,
                    amount_out: 1189351822990900207532202097059
                }]
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 139464963,
                block_timestamp_nanosec: 1739256255986699436,
                transaction_id: "CWm8Y6XgGdR2GHDJkzFkZHuG1uwicXEfbKpH41LWrB8E"
                    .parse()
                    .unwrap(),
                receipt_id: "3cN6x6opaVxnUBSGg8tR3oT2RFHTus3Y56p2oMKhLZ9D"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_grafun_sell() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(139464980..=139464990),
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
            .handler
            .pool_swaps
            .get(&"slimedragon.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            RawPoolSwap {
                pool: "GRAFUN-worm.gra-fun.near".to_owned(),
                token_in: "worm.gra-fun.near".parse().unwrap(),
                token_out: "wrap.near".parse().unwrap(),
                amount_in: 1189351822990900207532202097059,
                amount_out: 800000000000000000000001
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 139464984,
                block_timestamp_nanosec: 1739256278503057950,
                transaction_id: "9uJedNUnfKu9aGCWaugL6fHWujRchH1GpumUo4Cfdm9W"
                    .parse()
                    .unwrap(),
                receipt_id: "9rWnsbXPjChfgJVJi5rS8HqCTG4jCX9bRhyxqytGc69d"
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
                    ("wrap.near".parse().unwrap(), 800000000000000000000001),
                    (
                        "worm.gra-fun.near".parse().unwrap(),
                        -1189351822990900207532202097059
                    ),
                ]),
                pool_swaps: vec![RawPoolSwap {
                    pool: "GRAFUN-worm.gra-fun.near".to_owned(),
                    token_in: "worm.gra-fun.near".parse().unwrap(),
                    token_out: "wrap.near".parse().unwrap(),
                    amount_in: 1189351822990900207532202097059,
                    amount_out: 800000000000000000000001
                }],
            },
            TradeContext {
                trader: "slimedragon.near".parse().unwrap(),
                block_height: 139464984,
                block_timestamp_nanosec: 1739256278503057950,
                transaction_id: "9uJedNUnfKu9aGCWaugL6fHWujRchH1GpumUo4Cfdm9W"
                    .parse()
                    .unwrap(),
                receipt_id: "9rWnsbXPjChfgJVJi5rS8HqCTG4jCX9bRhyxqytGc69d"
                    .parse()
                    .unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_grafun_state_changes() {
    let mut indexer = TradeIndexer {
        handler: TestHandler::default(),
        is_testnet: false,
    };

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(139464980..=139464990),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert!(
        dbg!(indexer.handler.state_changes).contains(&PoolChangeEvent {
            pool_id: "GRAFUN-worm.gra-fun.near".to_owned(),
            receipt_id: "bhTe9zS9NEXCwbTR5FxoBhJ4LcTfyYspvP4CXwjYqcH"
                .parse()
                .unwrap(),
            block_timestamp_nanosec: 1739256276466427878,
            block_height: 139464982,
            pool: PoolType::GraFun(GraFunPool {
                token_id: "worm.gra-fun.near".parse().unwrap(),
                token_hold: 996148226856156196041971045291539,
                wnear_hold: 752899999999999999999999999,
                is_deployed: false,
                is_tradable: true,
            })
        })
    );
}
