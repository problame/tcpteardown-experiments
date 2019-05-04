use bytes::{BigEndian, ByteOrder};
use failure::ResultExt;
use log;
use std::io;
use std::io::prelude::*;
use std::net;
use std::net::TcpStream;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use structopt::StructOpt;

mod maybe_buffered;
use maybe_buffered::{split_tcp_stream, MaybeBufferedReader, MaybeBufferedWriter};

#[macro_use]
extern crate strum_macros;

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
}

#[derive(StructOpt)]
struct Server {
    listen: String,
    #[structopt(
        help = "`close-immediately`, `drain-then-close`, `shutdown-write-then-drain`, `sleep-then-close`, `shutdown-both-then-close`"
    )]
    teardown_mode: TeardownMode,
    #[structopt(long = "buffered")]
    buffered: bool,
    #[structopt(
        long = "sleep",
        help = "time to sleep when using `sleep-then-close` teardown mode",
        default_value = "5ms"
    )]
    sleep: humantime::Duration,

    /// Quote from socket(7) on the linger sockopt (Linux)
    /// When  enabled,  a close(2) or shutdown(2) will not return until all queued messages for the socket have
    /// been successfully sent or the linger timeout has been reached.  Otherwise, the call returns immediately
    /// and  the  closing  is  done in the background.  When the socket is closed as part of exit(2), it always
    /// lingers in the background.
    #[structopt(long = "linger")]
    linger: Option<humantime::Duration>,
}

#[derive(EnumString)]
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
    bind: String,
    connect: String,
    #[structopt(long = "times", default_value = "1")]
    times: usize,
    #[structopt(long = "buffered")]
    buffered: bool,
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
                    let (read, write) = split_tcp_stream(conn, self.buffered);
                    self.handle_conn(read, write)?;
                }
                Err(e) => log::error!("accept error: {:?}", e),
            }
        }
    }

    /// read numbers until we hit an odd number
    /// send the odd number back to the client, then tear-down the connection
    /// (the server configuration supports different kinds of tear-downs)
    fn handle_conn(
        &self,
        mut read: MaybeBufferedReader,
        write: MaybeBufferedWriter,
    ) -> Result<(), failure::Error> {
        let mut buf = vec![0 as u8; 4];
        loop {
            read.read_exact(&mut buf[..])
                .context("read from connection")?;
            let num = BigEndian::read_u32(&buf[..]);

            if num % 2 == 0 {
                continue;
            } else {
                log::info!("client sent odd number {:?}", num);
                let mut write: TcpStream = write
                    .unbuffered()
                    .context("convert buffered writer to unbuffered writer")?;
                write.write(&buf[..]).context("write error to connection")?;
                let mut read: TcpStream = read.unbuffered();

                match self.teardown_mode {
                    TeardownMode::CloseImmediately => {}
                    TeardownMode::SleepThenClose => {
                        spin_sleep::sleep(self.sleep.into());
                    }

                    TeardownMode::DrainThenClose => {
                        log::info!("draining connection");
                        let drained_bytes = self.drain(&mut read)?;
                        log::info!("drained {:?} bytes", drained_bytes);

                        log::info!("implicit drop & close of the connection");
                    }
                    TeardownMode::ShutdownWriteThenDrain => {
                        log::info!("shutting down write-end of the connection");
                        write.shutdown(net::Shutdown::Write).context("shutdown")?;

                        log::info!("draining connection");
                        let drained_bytes = self.drain(&mut read)?;
                        log::info!("drained {:?} bytes", drained_bytes);

                        log::info!("implicit drop & close of the connection");
                    }

                    TeardownMode::ShutdownWriteThenClose => {
                        time_and_log_debug!("shutdown write duration", {
                            write
                                .shutdown(net::Shutdown::Write)
                                .context("shutdown write")?;
                        });
                    }

                    TeardownMode::ShutdownBothThenClose => {
                        time_and_log_debug!("shutdown duration", {
                            write.shutdown(net::Shutdown::Both).context("shutdown")?;
                        });
                    }
                }
                time_and_log_debug!("close duration", {
                    drop(read);
                    drop(write);
                });

                return Ok(());
            }
        }
    }

    /// read & discard from the connection until EOF
    fn drain(&self, conn: &mut TcpStream) -> Result<u64, failure::Error> {
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
        log::info!("connecting to {:?}", self.connect);

        // Connect to the server
        use net2::unix::UnixTcpBuilderExt;
        let conn = net2::TcpBuilder::new_v4()
            .unwrap()
            .reuse_port(true)
            .context("reuse port")
            .unwrap()
            .bind(&self.bind)
            .context("cannot bind to specified address")
            .unwrap()
            .connect(&self.connect)
            .context("cannot connect to specified address")
            .unwrap();

        log::info!("connected {:?}", conn);
        let (mut read, mut write) = split_tcp_stream(conn, self.buffered);

        // Set to true by the response reader thread to indicate
        // that the number-write thread should stop sending numbers.
        let stop_sending = Arc::new(AtomicBool::new(false));

        // Start a thread that reads the server's response
        let server_response_reader = {
            let stop_sending = stop_sending.clone();
            std::thread::spawn(move || -> Result<u32, io::Error> {
                let mut buf = vec![0 as u8; 4];
                let res = read
                    .read_exact(&mut buf[..])
                    .map(|_| BigEndian::read_u32(&buf[..]));
                log::info!("server response received, stopping sender {:?}", res);
                stop_sending.store(true, atomic::Ordering::SeqCst);
                res
            })
        };

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
            let write_res = write.write_all(&buf[..]);
            if let Err(e) = write_res {
                write_err = Some(e);
                break;
            }
        }

        // Retrieve the response reader's result.
        let read_res: io::Result<u32> = server_response_reader.join().expect("thread panicked");
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
