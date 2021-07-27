use crate::kernel_connection::KernelServerState;
use crate::user_connection::UserServerState;
use crate::zettacache::ZettaCache;
use async_std::path::Path;
use log::*;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

pub fn setup_logging(verbosity: u64, file_name: Option<&str>) {
    let mut base_config = fern::Dispatch::new();

    base_config = match verbosity {
        0 => base_config.level(LevelFilter::Warn),
        1 => base_config
            .level(LevelFilter::Info)
            .level_for("rusoto_core::request", LevelFilter::Info)
            .level_for("want", LevelFilter::Debug),
        2 => base_config
            .level(LevelFilter::Debug)
            .level_for("rusoto_core::request", LevelFilter::Info)
            .level_for("want", LevelFilter::Debug),
        3 => base_config
            .level(LevelFilter::Trace)
            .level_for("rusoto_core::request", LevelFilter::Info)
            .level_for("want", LevelFilter::Debug),
        _ => base_config.level(LevelFilter::Trace),
    };

    let mut config = fern::Dispatch::new().format(|out, message, record| {
        let target = record.target();
        let stripped_target = target.strip_prefix("zoa_common").unwrap_or(target);
        out.finish(format_args!(
            "[{}][{}][{}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            stripped_target,
            record.level(),
            message
        ))
    });
    config = match file_name {
        None => config.chain(std::io::stdout()),
        Some(file_name) => config.chain(fern::log_file(file_name).unwrap()),
    };

    base_config.chain(config).apply().unwrap();
}

pub fn start(socket_dir: &str, cache_path: Option<&str>) {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("zoa")
        .build()
        .unwrap()
        .block_on(async move {
            UserServerState::start(socket_dir);

            let cache = match cache_path {
                Some(path) => Some(ZettaCache::open(path).await),
                None => None,
            };

            let id_path_name = format!("{}/zfs_agent_id", socket_dir);
            let id_path = Path::new(&id_path_name);

            let id = match File::open(id_path).await {
                Ok(mut f) => {
                    let mut bytes = Vec::new();
                    assert_eq!(
                        f.read_to_end(&mut bytes).await.unwrap(),
                        uuid::adapter::Hyphenated::LENGTH,
                    );
                    Uuid::parse_str(std::str::from_utf8(&bytes).unwrap()).unwrap()
                }
                Err(_) => {
                    let mut file = File::create(id_path).await.unwrap();
                    let uuid = Uuid::new_v4();
                    let mut buf = [0; uuid::adapter::Hyphenated::LENGTH];
                    uuid.to_hyphenated().encode_lower(&mut buf);
                    file.write_all(&buf).await.unwrap();
                    uuid
                }
            };

            KernelServerState::start(socket_dir, cache, id);

            // keep the process from exiting
            let () = futures::future::pending().await;
        });
}
