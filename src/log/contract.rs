//! Behaviour every PartitionLog must have. Called by each implementation's tests.

use crate::log::{LogError, PartitionLog};
use crate::record::Record;

pub async fn run_all(log: &dyn PartitionLog) {
    // empty log
    assert_eq!(log.high_watermark(), 0);
    assert_eq!(log.log_start(), 0);
    assert!(log.read(0, 1024).await.unwrap().is_empty());
    assert!(matches!(
        log.read(1, 1024).await,
        Err(LogError::OutOfRange(1))
    ));
    assert!(matches!(
        log.read(-1, 1024).await,
        Err(LogError::OutOfRange(-1))
    ));
    assert_eq!(log.append("p", vec![]).await.unwrap(), 0);

    // appends return base offsets
    let first = vec![Record::text(100, "a"), Record::text(300, "b")];
    assert_eq!(log.append("p", first.clone()).await.unwrap(), 0);
    assert_eq!(
        log.append("p", vec![Record::text(200, "c")]).await.unwrap(),
        2
    );
    assert_eq!(log.high_watermark(), 3);

    // reads return offsets and the original records
    let got = log.read(1, 1 << 20).await.unwrap();
    assert_eq!(
        got,
        vec![(1, first[1].clone()), (2, Record::text(200, "c"))]
    );
    assert!(log.read(3, 1024).await.unwrap().is_empty());

    // a record bigger than max_bytes is still returned, alone (KIP-74)
    let got = log.read(0, 1).await.unwrap();
    assert_eq!(got, vec![(0, first[0].clone())]);

    // timestamp lookup: first offset with ts >= target, scanning in offset order
    assert_eq!(log.offset_for_timestamp(50).await.unwrap(), Some((0, 100)));
    assert_eq!(log.offset_for_timestamp(150).await.unwrap(), Some((1, 300)));
    assert_eq!(log.offset_for_timestamp(250).await.unwrap(), Some((1, 300)));
    assert_eq!(log.offset_for_timestamp(301).await.unwrap(), None);
}
