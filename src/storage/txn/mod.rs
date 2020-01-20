// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Storage Transactions

pub mod commands;
pub mod sched_pool;
pub mod scheduler;

mod latch;
mod process;
mod store;

use crate::storage::{
    types::{MvccInfo, TxnStatus},
    Error as StorageError, Result as StorageResult,
};
use anyhow::Error as AnyError;
use kvproto::kvrpcpb::LockInfo;
use std::io::Error as IoError;
use txn_types::{Key, TimeStamp};

pub use self::commands::Command;
pub use self::process::RESOLVE_LOCK_BATCH_SIZE;
pub use self::scheduler::{Msg, Scheduler};
pub use self::store::{EntryBatch, TxnEntry, TxnEntryScanner, TxnEntryStore};
pub use self::store::{FixtureStore, FixtureStoreScanner};
pub use self::store::{Scanner, SnapshotStore, Store};

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<StorageResult<()>> },
    MvccKey { mvcc: MvccInfo },
    MvccStartTs { mvcc: Option<(Key, MvccInfo)> },
    Locks { locks: Vec<LockInfo> },
    TxnStatus { txn_status: TxnStatus },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
}

#[derive(Debug, thiserror::Error)]
#[unwrap]
pub enum Error {
    #[error(transparent)]
    Engine(#[from] crate::storage::kv::Error),

    #[error(transparent)]
    Codec(#[from] tikv_util::codec::Error),

    #[error(transparent)]
    Mvcc(#[from] crate::storage::mvcc::Error),

    #[error(transparent)]
    ProtoBuf(#[from] Box<protobuf::error::ProtobufError>),

    #[error(transparent)]
    Io(#[from] IoError),

    #[error("Invalid transaction tso with start_ts: {start_ts}, commit_ts: {commit_ts}")]
    InvalidTxnTso {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    },

    #[error("Request range exceeds bound, request range:[{}, end:{}), physical bound:[{}, {})",
        start.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
        end.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
        lower_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()),
        upper_bound.as_ref().map(hex::encode_upper).unwrap_or_else(|| "(none)".to_owned()))]
    InvalidReqRange {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
    },

    #[error(transparent)]
    Other(#[from] AnyError),
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match self {
            Error::Engine(e) => e.maybe_clone().map(Error::Engine),
            Error::Codec(e) => e.maybe_clone().map(Error::Codec),
            Error::Mvcc(e) => e.maybe_clone().map(Error::Mvcc),
            Error::InvalidTxnTso {
                start_ts,
                commit_ts,
            } => Some(Error::InvalidTxnTso {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
            }),
            Error::InvalidReqRange {
                start,
                end,
                lower_bound,
                upper_bound,
            } => Some(Error::InvalidReqRange {
                start: start.clone(),
                end: end.clone(),
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
            }),
            Error::Other(_) | Error::ProtoBuf(_) | Error::Io(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
