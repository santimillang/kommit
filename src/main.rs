use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use kommit::broker::Broker;
use kommit::config::Config;
use kommit::git::store::GitStore;
use kommit::storage::GitStorage;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

/// Kafka, except every record is a Git commit.
#[derive(Parser)]
#[command(version, args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    serve: Serve,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Fork a topic on a running broker. The new topic shares the source's history.
    Branch {
        /// The topic to fork.
        from: String,
        /// The new topic.
        to: String,
        /// `head`, an offset for every partition like `0:42,1:17`, or an RFC 3339 timestamp.
        #[arg(long, default_value = "head")]
        at: String,
        /// The broker to ask.
        #[arg(long, default_value = "localhost:9092")]
        bootstrap: String,
    },
}

/// Run the broker (the default when no subcommand is given).
#[derive(clap::Args)]
struct Serve {
    /// Bare Git repository holding all data (created if missing).
    #[arg(long, default_value = "kommit-data.git")]
    data: PathBuf,
    #[arg(long, default_value = "0.0.0.0:9092")]
    listen: SocketAddr,
    /// Host clients should connect to, as returned in Metadata.
    #[arg(long, default_value = "localhost")]
    advertised_host: String,
    /// Port returned in Metadata; defaults to the listening port.
    #[arg(long)]
    advertised_port: Option<i32>,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    auto_create_topics: bool,
    #[arg(long, default_value_t = 1)]
    default_partitions: i32,
    /// Milliseconds a new consumer group waits for more members before its first rebalance.
    #[arg(long, default_value_t = 3000)]
    group_initial_rebalance_delay_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("kommit=info")),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Branch {
            from,
            to,
            at,
            bootstrap,
        }) => {
            let starts = kommit::cli::branch(&bootstrap, &from, &to, &at).await?;
            for (p, offset) in starts.iter().enumerate() {
                println!(
                    "{to}/{p} starts at offset {offset}, sharing {offset} record(s) with {from}/{p}"
                );
            }
            Ok(())
        }
        None => serve(cli.serve).await,
    }
}

async fn serve(cli: Serve) -> anyhow::Result<()> {
    let store = Arc::new(GitStore::open_or_init(&cli.data)?);
    let listener = TcpListener::bind(cli.listen).await?;
    let config = Config {
        node_id: 0,
        advertised_host: cli.advertised_host,
        advertised_port: cli
            .advertised_port
            .unwrap_or(listener.local_addr()?.port() as i32),
        auto_create_topics: cli.auto_create_topics,
        default_partitions: cli.default_partitions,
        cluster_id: "kommit".into(),
        group_initial_rebalance_delay: std::time::Duration::from_millis(
            cli.group_initial_rebalance_delay_ms,
        ),
    };
    let broker = Broker::start(config, Arc::new(GitStorage::new(store))).await?;
    tracing::info!(data = %cli.data.display(), listen = %cli.listen, "kommit is up");
    kommit::net::server::serve(listener, broker).await
}
