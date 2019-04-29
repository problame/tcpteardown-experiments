use std::io::{self, BufReader, BufWriter};
use std::net::TcpStream;

pub fn split_tcp_stream(
    conn: TcpStream,
    buffered: bool,
) -> (MaybeBufferedReader, MaybeBufferedWriter) {
    let read = conn.try_clone().unwrap();
    let write = conn.try_clone().unwrap();
    if buffered {
        (
            MaybeBufferedReader::Buffered(BufReader::new(read)),
            MaybeBufferedWriter::Buffered(BufWriter::new(write)),
        )
    } else {
        (
            MaybeBufferedReader::Unbuffered(read),
            MaybeBufferedWriter::Unbuffered(write),
        )
    }
}

pub enum MaybeBufferedWriter {
    Unbuffered(TcpStream),
    Buffered(BufWriter<TcpStream>),
}

impl MaybeBufferedWriter {
    pub fn unbuffered(self) -> Result<TcpStream, io::IntoInnerError<BufWriter<TcpStream>>> {
        match self {
            MaybeBufferedWriter::Unbuffered(s) => Ok(s),
            MaybeBufferedWriter::Buffered(bw) => bw.into_inner(),
        }
    }
}

impl std::ops::Deref for MaybeBufferedWriter {
    type Target = io::Write;
    fn deref(&self) -> &Self::Target {
        match self {
            MaybeBufferedWriter::Unbuffered(s) => s,
            MaybeBufferedWriter::Buffered(b) => b,
        }
    }
}

impl std::ops::DerefMut for MaybeBufferedWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            MaybeBufferedWriter::Unbuffered(s) => s,
            MaybeBufferedWriter::Buffered(b) => b,
        }
    }
}

pub enum MaybeBufferedReader {
    Unbuffered(TcpStream),
    Buffered(BufReader<TcpStream>),
}

impl MaybeBufferedReader {
    pub fn unbuffered(self) -> TcpStream {
        match self {
            MaybeBufferedReader::Unbuffered(s) => s,
            MaybeBufferedReader::Buffered(br) => br.into_inner(),
        }
    }
}

impl std::ops::Deref for MaybeBufferedReader {
    type Target = io::Read;
    fn deref(&self) -> &Self::Target {
        match self {
            MaybeBufferedReader::Unbuffered(s) => s,
            MaybeBufferedReader::Buffered(b) => b,
        }
    }
}

impl std::ops::DerefMut for MaybeBufferedReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            MaybeBufferedReader::Unbuffered(s) => s,
            MaybeBufferedReader::Buffered(b) => b,
        }
    }
}
