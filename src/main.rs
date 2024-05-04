use trade_indexer::redis_handler::PushToRedisStream;

use inindexer::fastnear_data_server::FastNearDataServerProvider;
use inindexer::{
    run_indexer, AutoContinue, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use redis::aio::ConnectionManager;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let mut indexer = trade_indexer::TradeIndexer(PushToRedisStream::new(connection, 100_000));

    #[cfg(feature = "parallel")]
    let streamer = inindexer::message_provider::ParallelProviderStreamer::new(
        FastNearDataServerProvider::mainnet(),
        10,
    );
    #[cfg(not(feature = "parallel"))]
    let streamer = FastNearDataServerProvider::mainnet();

    run_indexer(
        &mut indexer,
        streamer,
        IndexerOptions {
            range: if std::env::args().len() > 1 {
                // For debugging
                let msg = "Usage: `trade-indexer` or `trade-indexer [start-block] [end-block]`";
                BlockIterator::iterator(
                    std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg)
                        ..=std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                )
            } else {
                BlockIterator::AutoContinue(AutoContinue::default())
            },
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Indexer run failed");
}
