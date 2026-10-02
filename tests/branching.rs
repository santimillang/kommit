mod common;

use std::sync::Arc;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::create_topics_request::CreatableTopicConfig;
use kafka_protocol::protocol::StrBytes;
use kommit::api::records::{decode_batches, encode_batch};
use kommit::branch::{BranchAt, BranchSpec};
use kommit::broker::{Broker, TopicState};
use kommit::config::Config;
use kommit::git::store::GitStore;
use kommit::record::Record;
use kommit::storage::GitStorage;

const INVALID_CONFIG: i16 = 40;

async fn produce(c: &mut common::TestClient, topic: &str, p: i32, recs: &[(i64, &str)]) {
    let batch: Vec<_> = recs
        .iter()
        .enumerate()
        .map(|(i, (ts, v))| (i as i64, Record::text(*ts, v)))
        .collect();
    let r = c
        .send(
            9,
            common::produce_req(topic, p, encode_batch(&batch).unwrap(), 1),
        )
        .await;
    assert_eq!(r.responses[0].partition_responses[0].error_code, 0);
}

async fn values(c: &mut common::TestClient, topic: &str, p: i32) -> Vec<String> {
    let r = c.send(12, common::fetch_req(topic, p, 0, 0, 1 << 20)).await;
    let part = &r.responses[0].partitions[0];
    assert_eq!(part.error_code, 0, "fetch {topic}/{p}");
    decode_batches(part.records.clone().unwrap_or_default())
        .unwrap()
        .into_iter()
        .map(|(_, r)| String::from_utf8(r.value.unwrap().to_vec()).unwrap())
        .collect()
}

async fn branch(
    c: &mut common::TestClient,
    name: &str,
    from: &str,
    at: Option<&str>,
) -> (i16, String) {
    let r = c.send(7, common::branch_topic_req(name, from, at)).await;
    let t = &r.topics[0];
    (
        t.error_code,
        t.error_message
            .as_ref()
            .map(|m| m.to_string())
            .unwrap_or_default(),
    )
}

#[tokio::test]
async fn a_branch_replays_the_source_and_diverges_from_it() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 2).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    produce(&mut c, "orders", 0, &[(1, "a"), (2, "b")]).await;
    produce(&mut c, "orders", 1, &[(1, "c")]).await;

    let r = c
        .send(7, common::branch_topic_req("replay", "orders", None))
        .await;
    assert_eq!(r.topics[0].error_code, 0);
    assert_eq!(r.topics[0].num_partitions, 2);
    assert_eq!(values(&mut c, "replay", 0).await, ["a", "b"]);
    assert_eq!(values(&mut c, "replay", 1).await, ["c"]);

    produce(&mut c, "replay", 0, &[(3, "mine")]).await;
    produce(&mut c, "orders", 0, &[(3, "theirs")]).await;
    assert_eq!(values(&mut c, "replay", 0).await, ["a", "b", "mine"]);
    assert_eq!(values(&mut c, "orders", 0).await, ["a", "b", "theirs"]);
}

#[tokio::test]
async fn a_branch_starts_at_offsets_or_a_timestamp() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    produce(&mut c, "orders", 0, &[(100, "a"), (200, "b"), (300, "c")]).await;

    assert_eq!(
        branch(&mut c, "by-offset", "orders", Some("0:1")).await.0,
        0
    );
    assert_eq!(values(&mut c, "by-offset", 0).await, ["a"]);
    // 1970-01-01T00:00:00.200Z: everything before the first record at or after 200 ms
    let at = Some("1970-01-01T00:00:00.200Z");
    assert_eq!(branch(&mut c, "by-time", "orders", at).await.0, 0);
    assert_eq!(values(&mut c, "by-time", 0).await, ["a"]);
    let at = Some("2100-01-01T00:00:00Z");
    assert_eq!(branch(&mut c, "later", "orders", at).await.0, 0);
    assert_eq!(values(&mut c, "later", 0).await, ["a", "b", "c"]);
    assert_eq!(branch(&mut c, "empty", "orders", Some("0:0")).await.0, 0);
    assert!(values(&mut c, "empty", 0).await.is_empty());
}

#[tokio::test]
async fn bad_branch_requests_are_refused_with_a_reason() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 2).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    for (name, from, at, needle) in [
        ("r1", "nope", None, "nope does not exist"),
        ("r2", "orders", Some("tail"), "expected `head`"),
        ("r3", "orders", Some("0:0"), "no offset for partition 1"),
        ("r4", "orders", Some("0:5,1:0"), "cannot branch at offset 5"),
    ] {
        let (code, msg) = branch(&mut c, name, from, at).await;
        assert_eq!(code, INVALID_CONFIG, "{name}: {msg}");
        assert!(msg.contains(needle), "{name}: {msg}");
    }
    let (code, _) = branch(&mut c, "bad name", "orders", None).await;
    assert_eq!(code, ResponseError::InvalidTopicException.code());
    let (code, _) = branch(&mut c, "orders", "orders", None).await;
    assert_eq!(code, ResponseError::TopicAlreadyExists.code());

    // an explicit, different partition count
    let mut req = common::branch_topic_req("r5", "orders", None);
    req.topics[0].num_partitions = 3;
    let r = c.send(7, req).await;
    assert_eq!(r.topics[0].error_code, INVALID_CONFIG);
    // like Kafka, a failed result leaves the counts unknown
    assert_eq!(
        (r.topics[0].num_partitions, r.topics[0].replication_factor),
        (-1, -1)
    );
    // the matching count is fine
    let mut req = common::branch_topic_req("r6", "orders", None);
    req.topics[0].num_partitions = 2;
    assert_eq!(c.send(7, req).await.topics[0].error_code, 0);

    // an unknown kommit.* key on a plain create
    let mut req = common::create_topic_req("r7", 1);
    req.topics[0].configs = vec![
        CreatableTopicConfig::default()
            .with_name(StrBytes::from_static_str("kommit.branch.form"))
            .with_value(Some(StrBytes::from_static_str("orders"))),
    ];
    let r = c.send(7, req).await;
    assert_eq!(r.topics[0].error_code, INVALID_CONFIG);

    // validate_only checks everything and creates nothing
    let mut req = common::branch_topic_req("dry", "orders", None);
    req.validate_only = true;
    let r = c.send(7, req).await;
    assert_eq!((r.topics[0].error_code, r.topics[0].num_partitions), (0, 2));
    assert!(broker.topic("dry").await.is_none());

    for name in ["r1", "r2", "r3", "r4", "r5", "r7"] {
        assert!(broker.topic(name).await.is_none(), "{name} was created");
    }
}

async fn git_broker(path: &std::path::Path) -> Arc<Broker> {
    let store = Arc::new(GitStore::open_or_init(path).unwrap());
    Broker::start(Config::for_tests(), Arc::new(GitStorage::new(store)))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_branch_can_take_consumer_groups_along() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    produce(&mut c, "orders", 0, &[(1, "a"), (2, "b"), (3, "c")]).await;
    let orders = broker.topic("orders").await.unwrap();
    orders.partitions[0]
        .commit_offset("billing", 1)
        .await
        .unwrap();
    orders.partitions[0]
        .commit_offset("audit", 3)
        .await
        .unwrap();

    let with_groups = |name: &str, groups: &str| {
        let mut req = common::branch_topic_req(name, "orders", Some("0:2"));
        req.topics[0].configs.push(
            CreatableTopicConfig::default()
                .with_name(StrBytes::from_static_str("kommit.branch.groups"))
                .with_value(Some(StrBytes::from_string(groups.to_string()))),
        );
        req
    };
    let committed = |t: Arc<TopicState>, g: &'static str| async move {
        t.partitions[0].committed_offset(g).await.unwrap()
    };

    assert_eq!(
        c.send(7, with_groups("all", "all")).await.topics[0].error_code,
        0
    );
    let all = broker.topic("all").await.unwrap();
    assert_eq!(committed(all.clone(), "billing").await, Some(1));
    // audit had read past the fork point (2): caught up on the fork
    assert_eq!(committed(all, "audit").await, Some(2));

    assert_eq!(
        c.send(7, with_groups("one", "billing")).await.topics[0].error_code,
        0
    );
    let one = broker.topic("one").await.unwrap();
    assert_eq!(committed(one.clone(), "billing").await, Some(1));
    assert_eq!(committed(one, "audit").await, None);

    // the default takes none
    assert_eq!(branch(&mut c, "plain", "orders", None).await.0, 0);
    let plain = broker.topic("plain").await.unwrap();
    assert_eq!(committed(plain, "billing").await, None);

    let r = c.send(7, with_groups("ghost", "billing,ghost")).await;
    assert_eq!(r.topics[0].error_code, INVALID_CONFIG);
    let msg = r.topics[0].error_message.as_ref().unwrap().to_string();
    assert!(msg.contains("ghost"), "{msg}");
    assert!(broker.topic("ghost").await.is_none());
}

#[tokio::test]
async fn forks_of_forks_survive_a_restart_and_share_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.git");
    let spec = |from: &str, at: &str| BranchSpec {
        from: from.into(),
        at: BranchAt::parse(at).unwrap(),
        groups: kommit::branch::BranchGroups::None,
    };
    {
        let broker = git_broker(&path).await;
        broker.create_topic("orders", 1).await.unwrap();
        let orders = broker.topic("orders").await.unwrap();
        let recs = (0..3).map(|i| Record::text(i, &format!("r{i}"))).collect();
        orders.partitions[0].append("app", recs).await.unwrap();
        broker
            .branch_topic("replay", -1, &spec("orders", "0:2"), false)
            .await
            .unwrap();
        let replay = broker.topic("replay").await.unwrap();
        assert_eq!(replay.root, "orders");
        replay.partitions[0]
            .append("app", vec![Record::text(9, "x")])
            .await
            .unwrap();
        replay.partitions[0].commit_offset("g", 1).await.unwrap();
        broker
            .branch_topic("replay2", -1, &spec("replay", "head"), false)
            .await
            .unwrap();
        assert_eq!(broker.topic("replay2").await.unwrap().root, "orders");
    }
    let broker = git_broker(&path).await;
    let hw = |t: Arc<TopicState>| t.partitions[0].high_watermark();
    assert_eq!(hw(broker.topic("orders").await.unwrap()), 3);
    assert_eq!(hw(broker.topic("replay").await.unwrap()), 3);
    assert_eq!(hw(broker.topic("replay2").await.unwrap()), 3);
    assert_eq!(broker.topic("replay2").await.unwrap().root, "orders");

    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .arg("--git-dir")
            .arg(&path)
            .args(args)
            .output()
            .unwrap();
        assert!(out.status.success(), "{args:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    assert_eq!(
        git(&["rev-parse", "replay2/0"]),
        git(&["rev-parse", "replay/0"])
    );
    assert_eq!(
        git(&["merge-base", "orders/0", "replay/0"]),
        git(&["rev-parse", "orders/0~1"])
    );
    // a group's lag on a fork is still git rev-list --count
    assert_eq!(
        git(&["rev-list", "--count", "refs/groups/g/replay/0..replay/0"]),
        "2"
    );
    git(&["fsck", "--strict", "--no-dangling"]);
}

#[tokio::test]
async fn the_cli_library_branches_and_reports_fork_points() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 2).await.unwrap();
    let mut c = common::TestClient::connect(addr).await;
    produce(&mut c, "orders", 0, &[(1, "a"), (2, "b")]).await;

    let hws = kommit::cli::branch(&addr.to_string(), "orders", "replay", "0:1,1:0")
        .await
        .unwrap();
    assert_eq!(hws, vec![1, 0]);
    let err = kommit::cli::branch(&addr.to_string(), "nope", "x", "head")
        .await
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("nope does not exist"), "{msg}");
    assert!(msg.contains("InvalidConfig"), "{msg}");
}

#[tokio::test]
async fn the_cli_client_gives_up_on_a_silent_broker() {
    // accepts the connection, then never answers
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _held = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        drop(socket);
    });
    let mut client = kommit::net::client::Client::connect(&addr.to_string())
        .await
        .unwrap()
        .with_timeout(std::time::Duration::from_millis(200));
    let started = std::time::Instant::now();
    let err = client
        .send(3, kafka_protocol::messages::ApiVersionsRequest::default())
        .await
        .err()
        .unwrap();
    assert!(format!("{err:#}").contains("timed out"), "{err:#}");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

#[tokio::test]
async fn the_kommit_binary_has_a_branch_subcommand() {
    let (addr, broker) = common::start_mem_broker().await;
    broker.create_topic("orders", 1).await.unwrap();
    let run = || {
        tokio::process::Command::new(env!("CARGO_BIN_EXE_kommit"))
            .args([
                "branch",
                "orders",
                "replay",
                "--bootstrap",
                &addr.to_string(),
            ])
            .output()
    };
    let out = run().await.unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("replay/0 starts at offset 0"), "{stdout}");
    assert!(broker.topic("replay").await.is_some());

    let out = run().await.unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("already exists"), "{stderr}");
}
