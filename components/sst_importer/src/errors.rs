// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::io::Error as IoError;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::result;

use grpcio::Error as GrpcError;
use tokio_sync::oneshot::error::RecvError;
use uuid::{parser::ParseError, BytesError};

use crate::metrics::*;

pub fn error_inc(err: &Error) {
    let label = match err {
        Error::Io(..) => "io",
        Error::Grpc(..) => "grpc",
        Error::Uuid(..) => "uuid",
        Error::UuidBytes(..) => "uuid_bytes",
        Error::RocksDB(..) => "rocksdb",
        Error::EngineTraits(..) => "engine_traits",
        Error::ParseIntError(..) => "parse_int",
        Error::FileExists(..) => "file_exists",
        Error::FileCorrupted(..) => "file_corrupt",
        Error::InvalidSSTPath(..) => "invalid_sst",
        Error::Engine(..) => "engine",
        Error::CannotReadExternalStorage(..) => "read_external_storage",
        Error::WrongKeyPrefix(..) => "wrong_prefix",
        Error::BadFormat(..) => "bad_format",
        _ => return,
    };
    IMPORTER_ERROR_VEC.with_label_values(&[label]).inc();
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Grpc(err: GrpcError) {
            from()
            cause(err)
            description(err.description())
        }
        Uuid(err: ParseError) {
            from()
            cause(err)
            description(err.description())
        }
        UuidBytes(err: BytesError) {
            from()
            cause(err)
            description(err.description())
        }
        Future(err: RecvError) {
            from()
            cause(err)
        }
        // FIXME: Remove concrete 'rocks' type
        RocksDB(msg: String) {
            from()
            display("RocksDB {}", msg)
        }
        EngineTraits(err: engine_traits::Error) {
            from()
            description("Engine error")
            display("Engine {:?}", err)
        }
        ParseIntError(err: ParseIntError) {
            from()
            cause(err)
            description(err.description())
        }
        FileExists(path: PathBuf) {
            display("File {:?} exists", path)
        }
        FileCorrupted(path: PathBuf, reason: String) {
            display("File {:?} corrupted: {}", path, reason)
        }
        InvalidSSTPath(path: PathBuf) {
            display("Invalid SST path {:?}", path)
        }
        InvalidChunk {}
        Engine(err: Box<dyn StdError + Send + Sync + 'static>) {
            display("{}", err)
        }
        CannotReadExternalStorage(url: String, name: String, err: IoError) {
            cause(err)
            display("Cannot read {}/{}", url, name)
        }
        WrongKeyPrefix(what: &'static str, key: Vec<u8>, prefix: Vec<u8>) {
            display("\
                {} has wrong prefix: key {} does not start with {}",
                what,
                hex::encode_upper(&key),
                hex::encode_upper(&prefix),
            )
        }
        BadFormat(msg: String) {
            display("bad format {}", msg)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
