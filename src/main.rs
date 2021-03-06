use std::io::{self, prelude::*, BufReader, BufWriter};
use std::net::{self, TcpStream};
use std::sync::{
    atomic::{self, AtomicBool},
    Arc,
};

use bytes::{BigEndian, ByteOrder};
use failure::ResultExt;
use log;
use net2::unix::UnixTcpBuilderExt;
use structopt::StructOpt;
#[macro_use]
extern crate strum_macros;
use strum::IntoEnumIterator;

/// macro used to measure & log the duration of a given expression
macro_rules! time_and_log_debug {
    ($name:expr, $e:expr) => {{
        let pre = std::time::Instant::now();
        let res = $e;
        let post = std::time::Instant::now() - pre;
        log::debug!("{:?}: {:?}", $name, post);
        res
    }};
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum App {
    Server(Server),
    Client(Client),
    Modes,
}

#[derive(StructOpt)]
struct Server {
    #[structopt(help = "bind listening to socket to IP:port")]
    listen: String,
    #[structopt(help = "use `modes` subcommand to list modes")]
    teardown_mode: TeardownMode,
    #[structopt(
        long = "sleep",
        help = "time to sleep for teardown modes that sleep",
        default_value = "5ms"
    )]
    sleep: humantime::Duration,
    #[structopt(
        long = "linger",
        help = "enable lingering for client connections (e.g. `2s`)"
    )]
    linger: Option<humantime::Duration>,
}

#[derive(EnumString, EnumIter, Display)]
#[strum(serialize_all = "kebab_case")]
enum TeardownMode {
    CloseImmediately,
    DrainThenClose,
    ShutdownWriteThenDrain,
    ShutdownWriteThenClose,
    SleepThenClose,
    ShutdownBothThenClose,
}

#[derive(StructOpt)]
struct Client {
    #[structopt(help = "SERVER_IP:SERVER_PORT")]
    server: String,
    #[structopt(long = "bind", help = "bind connecting socket to address IP:port")]
    bind: Option<String>,
    #[structopt(long = "times", default_value = "1")]
    times: usize,
}

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    let m = App::from_args();
    match m.run() {
        Ok(()) => (),
        Err(e) => eprintln!("error: {:?}", e),
    }
}

impl App {
    fn run(&self) -> Result<(), failure::Error> {
        match self {
            App::Server(s) => s.run(),
            App::Client(c) => c.run(),
            App::Modes => {
                TeardownMode::iter().for_each(|e| println!("{}", e));
                Ok(())
            }
        }
    }
}

impl Server {
    fn run(&self) -> Result<(), failure::Error> {
        let listener = net::TcpListener::bind(&self.listen).context("bind")?;
        log::info!("listening on {:?}", listener.local_addr());

        loop {
            log::info!("accepting connection");
            let conn = listener.incoming().next().unwrap();
            match conn.context("accept") {
                Ok(conn) => {
                    log::info!("accepted connection {:?}", conn);
                    use net2::TcpStreamExt;
                    conn.set_linger(self.linger.map(|hd| hd.into()))?;
                    self.handle_conn(conn)?;
                }
                Err(e) => log::error!("accept error: {:?}", e),
            }
        }
    }

    fn handle_conn(&self, mut conn: TcpStream) -> Result<(), failure::Error> {
        // buffer for number
        let mut buf = vec![0 as u8; 4];

        // read from the connection until we encounter the first odd number
        let first_odd_num = {
            // use buffered I/O to avoid a syscall every iteration of the loop
            let mut conn = BufReader::new(&mut conn);

            loop {
                conn.read_exact(&mut buf[..])
                    .context("read from connection")?;
                let num = BigEndian::read_u32(&buf[..]);

                if num % 2 == 0 {
                    continue;
                } else {
                    log::info!("client sent odd number {:?}", num);
                    break num;
                }
            }
        };

        // send the odd number back to the client
        BigEndian::write_u32(&mut buf, first_odd_num);
        conn.write(&buf).context("write odd number to connection")?;

        // close the connection according to parameter
        match self.teardown_mode {
            TeardownMode::CloseImmediately => {}
            TeardownMode::SleepThenClose => {
                spin_sleep::sleep(self.sleep.into());
            }

            TeardownMode::DrainThenClose => {
                log::info!("draining connection");
                let drained_bytes = Self::drain(&mut conn)?;
                log::info!("drained {:?} bytes", drained_bytes);

                log::info!("implicit drop & close of the connection");
            }
            TeardownMode::ShutdownWriteThenDrain => {
                log::info!("shutting down write-end of the connection");
                conn.shutdown(net::Shutdown::Write).context("shutdown")?;

                log::info!("draining connection");
                let drained_bytes = Self::drain(&mut conn)?;
                log::info!("drained {:?} bytes", drained_bytes);

                log::info!("implicit drop & close of the connection");
            }

            TeardownMode::ShutdownWriteThenClose => {
                time_and_log_debug!("shutdown write duration", {
                    conn.shutdown(net::Shutdown::Write)
                        .context("shutdown write")?;
                });
            }

            TeardownMode::ShutdownBothThenClose => {
                time_and_log_debug!("shutdown duration", {
                    conn.shutdown(net::Shutdown::Both).context("shutdown")?;
                });
            }
        }
        time_and_log_debug!("close duration", {
            drop(conn);
        });

        Ok(())
    }

    /// read & discard from the connection until EOF
    fn drain(conn: &mut TcpStream) -> Result<u64, failure::Error> {
        let mut bytecount = 0;
        let mut buf = vec![0 as u8; 1 << 15];
        loop {
            match conn.read(&mut buf) {
                Ok(0) => return Ok(bytecount),
                Ok(n) => bytecount += n as u64,
                Err(e) => {
                    log::debug!("error while draining: {:?}", e);
                    return Err(e).context("read from connection")?;
                }
            }
        }
    }
}

#[derive(Debug, Display, Hash, PartialEq, Eq, PartialOrd)]
enum SingleRunResult {
    ResponseCorrect,
    ReadResponseError(io::ErrorKind),
    WriteNumberError(io::ErrorKind),
    BothErr {
        read: io::ErrorKind,
        write: io::ErrorKind,
    },
}

impl Client {
    fn run(&self) -> Result<(), failure::Error> {
        let mut stats = std::collections::HashMap::new();
        for _ in 0..self.times {
            let res = self.single_run();
            log::info!("run result: {:?}", res);
            let e = stats.entry(res).or_insert(0);
            *e += 1;
        }
        println!("multi run stats:\n{:#?}", stats);
        Ok(())
    }

    fn single_run(&self) -> SingleRunResult {
        log::info!("connecting to {:?}", self.server);

        // Connect to the server
        let conn = {
            let builder = net2::TcpBuilder::new_v4().unwrap();
            builder.reuse_port(true).expect("reuse port");
            if let Some(bind) = &self.bind {
                builder
                    .bind(bind)
                    .expect("cannot bind to specified address");
            }
            builder
                .connect(&self.server)
                .expect("cannot connect to specified address")
        };
        log::info!("connected {:?}", conn);

        // Set to true by the response reader thread to indicate
        // that the number-write thread should stop sending numbers.
        let stop_sending = Arc::new(AtomicBool::new(false));

        // Start a thread that reads the server's response
        let server_response_reader = {
            let stop_sending = stop_sending.clone();
            let mut conn = conn.try_clone().expect("cannot clone connection handle");
            std::thread::spawn(move || -> Result<u32, io::Error> {
                let mut buf = vec![0 as u8; 4];
                let res = conn
                    .read_exact(&mut buf[..])
                    .map(|_| BigEndian::read_u32(&buf[..]));
                log::info!("server response received, stopping sender {:?}", res);
                stop_sending.store(true, atomic::Ordering::SeqCst);
                res
            })
        };

        let mut buffered_conn = BufWriter::new(conn);
        let mut buf = vec![0 as u8; 4];
        let send_numbers_count = 1 << 23; // => will send at most 8 * 4 MiB numbers
        let mut write_err: Option<io::Error> = None;
        for mut i in 0..send_numbers_count {
            // Did the response reader thread receive a response?
            if stop_sending.load(atomic::Ordering::SeqCst) {
                log::info!("stop sending numbers");
                break;
            }

            if i == send_numbers_count / 2 {
                // We are in the middle of the number stream.
                // Up until now, we only sent even numbers.
                // Now send a single odd number, then proceed with even numbers.
                i = 23;
            } else {
                // Produce even numbers by rounding down.
                i &= &(!1);
            }
            BigEndian::write_u32(&mut buf, i);

            // Try to send the number. Stop sending numbers if an error occurs,
            // and remember that error.
            let write_res = buffered_conn.write_all(&buf[..]);
            if let Err(e) = write_res {
                write_err = Some(e);
                break;
            }
        }

        // Retrieve the response reader's result.
        let read_res: io::Result<u32> = server_response_reader
            .join()
            .expect("receiver thread panicked");
        let read_err: Option<io::Error> = read_res.map(|_num| ()).err();

        // Categorize what we observed in this run (used for statistics)
        match (read_err, write_err) {
            (None, None) => SingleRunResult::ResponseCorrect,
            (Some(e), None) => SingleRunResult::ReadResponseError(e.kind()),
            (None, Some(e)) => SingleRunResult::WriteNumberError(e.kind()),
            (Some(read), Some(write)) => SingleRunResult::BothErr {
                read: read.kind(),
                write: write.kind(),
            },
        }
    }
}
