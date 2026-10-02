mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kommit::api::records::{decode_batches, encode_batch};
use kommit::git::store::GitStore;
use kommit::record::Record;
use kommit::storage::GitStorage;

#[tokio::test]
async fn records_survive_a_broker_restart_over_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.git");

    {
        let store = Arc::new(GitStore::open_or_init(&path).unwrap());
        let (addr, broker, server) =
            common::start_broker_with_handle(Arc::new(GitStorage::new(store))).await;
        let mut c = common::TestClient::connect(addr).await;
        c.send(7, common::create_topic_req("orders", 1)).await;
        let batch = encode_batch(&[(0, Record::text(1, "before restart"))]).unwrap();
        let r = c.send(9, common::produce_req("orders", 0, batch, 1)).await;
        assert_eq!(r.responses[0].partition_responses[0].error_code, 0);
        // Shut the first broker down: stop accepting, close our connection, drop our handle.
        server.abort();
        let _ = server.await;
        drop(c);
        drop(broker);
    }

    // The connection task notices EOF asynchronously and only then releases the
    // last Arc<GitStore> (and the lock). Retry briefly instead of sleeping blindly.
    let store = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match GitStore::open_or_init(&path) {
                Ok(s) => break Arc::new(s),
                Err(e)
                    if Instant::now() < deadline
                        && e.to_string().contains("another kommit broker") =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await
                }
                Err(e) => panic!("reopen failed: {e:#}"),
            }
        }
    };
    let (addr, _broker) = common::start_broker(Arc::new(GitStorage::new(store))).await;
    let mut c = common::TestClient::connect(addr).await;
    let r = c
        .send(12, common::fetch_req("orders", 0, 0, 0, 1 << 20))
        .await;
    let got = decode_batches(r.responses[0].partitions[0].records.clone().unwrap()).unwrap();
    assert_eq!(got, vec![(0, Record::text(1, "before restart"))]);
}
