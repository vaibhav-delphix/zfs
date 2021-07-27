use clap::Arg;
use log::*;
use zoa_common::init;

fn main() {
    let matches = clap::App::new("ZFS Object Agent")
        .about("Enables the ZFS kernel module talk to S3-protocol object storage")
        .arg(
            Arg::with_name("verbosity")
                .short("v")
                .multiple(true)
                .help("Sets the level of logging verbosity"),
        )
        .arg(
            Arg::with_name("socket-dir")
                .short("d")
                .long("socket-dir")
                .value_name("DIR")
                .help("Directory for unix-domain sockets")
                .takes_value(true)
                .default_value("/run"),
        )
        .arg(
            Arg::with_name("output-file")
                .short("o")
                .long("output-file")
                .value_name("FILE")
                .help("File to log output to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("cache-file")
                .short("c")
                .long("cache-file")
                .value_name("FILE")
                .help("File/device to use for ZettaCache")
                .takes_value(true),
        )
        .get_matches();

    let socket_dir = matches.value_of("socket-dir").unwrap();

    init::setup_logging(
        matches.occurrences_of("verbosity"),
        matches.value_of("output-file"),
    );
    let cache_path = matches.value_of("cache-file");

    error!(
        "Starting ZFS Object Agent.  Local timezone is {}",
        chrono::Local::now().format("%Z (%:z)")
    );

    // error!() should be used when an invalid state is encountered; the related
    // operation will fail and the program may exit.  E.g. an invalid request
    // was received from the client (kernel).
    error!("logging level ERROR enabled");

    // warn!() should be used when something unexpected has happened, but it can
    // be recovered from.
    warn!("logging level WARN enabled");

    // info!() should be used for very high level operations which are expected
    // to happen infrequently (no more than once per minute in typical
    // operation).  e.g. opening/closing a pool, long-lived background tasks,
    // things that might be in `zpool history -i`.
    info!("logging level INFO enabled");

    // debug!() can be used for all but the most frequent operations.
    // e.g. not every single read/write/free operation, but perhaps for every
    // call to S3.
    debug!("logging level DEBUG enabled");

    // trace!() can be used indiscriminately.
    trace!("logging level TRACE enabled");

    init::start(socket_dir, cache_path);
}
