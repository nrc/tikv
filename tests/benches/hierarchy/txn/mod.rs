// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use concurrency_manager::ConcurrencyManager;
use criterion::{black_box, BatchSize, Bencher, Criterion};
use kvproto::kvrpcpb::Context;
use test_util::KvGenerator;
use tikv::storage::kv::{Engine, WriteData};
use tikv::storage::mvcc::{self, MvccTxn, SnapshotReader};
use txn_types::{Key, Mutation, TimeStamp};

use super::{BenchConfig, EngineFactory, DEFAULT_ITERATIONS};
use tikv::storage::txn::{
    cleanup, commit, prewrite, CommitKind, TransactionKind, TransactionProperties,
};

fn setup_prewrite<E, F>(
    engine: &E,
    config: &BenchConfig<F>,
    start_ts: impl Into<TimeStamp>,
) -> Vec<Key>
where
    E: Engine,
    F: EngineFactory<E>,
{
    let ctx = Context::default();

    let snapshot = engine.snapshot(Default::default()).unwrap();
    let start_ts = start_ts.into();
    let cm = ConcurrencyManager::new(start_ts);
    let mut txn = MvccTxn::new(start_ts, cm);
    let mut reader = SnapshotReader::new(start_ts, snapshot, true);

    let kvs = KvGenerator::new(config.key_length, config.value_length).generate(DEFAULT_ITERATIONS);
    for (k, v) in &kvs {
        let txn_props = TransactionProperties {
            start_ts: TimeStamp::default(),
            kind: TransactionKind::Optimistic(false),
            commit_kind: CommitKind::TwoPc,
            primary: &k.clone(),
            txn_size: 0,
            lock_ttl: 0,
            min_commit_ts: TimeStamp::default(),
            need_old_value: false,
        };
        prewrite(
            &mut txn,
            &mut reader,
            &txn_props,
            Mutation::Put((Key::from_raw(&k), v.clone())),
            &None,
            false,
        )
        .unwrap();
    }
    let write_data = WriteData::from_modifies(txn.into_modifies());
    let _ = engine.write(&ctx, write_data);
    let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
    keys
}

fn txn_prewrite<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || {
            let mutations: Vec<(Mutation, Vec<u8>)> =
                KvGenerator::new(config.key_length, config.value_length)
                    .generate(DEFAULT_ITERATIONS)
                    .iter()
                    .map(|(k, v)| (Mutation::Put((Key::from_raw(&k), v.clone())), k.clone()))
                    .collect();
            mutations
        },
        |mutations| {
            for (mutation, primary) in mutations {
                let snapshot = engine.snapshot(Default::default()).unwrap();
                let mut txn = mvcc::MvccTxn::new(1.into(), cm.clone());
                let mut reader = SnapshotReader::new(1.into(), snapshot, true);
                let txn_props = TransactionProperties {
                    start_ts: TimeStamp::default(),
                    kind: TransactionKind::Optimistic(false),
                    commit_kind: CommitKind::TwoPc,
                    primary: &primary,
                    txn_size: 0,
                    lock_ttl: 0,
                    min_commit_ts: TimeStamp::default(),
                    need_old_value: false,
                };
                prewrite(&mut txn, &mut reader, &txn_props, mutation, &None, false).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_commit<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(Default::default()).unwrap();
                let mut txn = mvcc::MvccTxn::new(1.into(), cm.clone());
                let mut reader = SnapshotReader::new(1.into(), snapshot, true);
                commit(&mut txn, &mut reader, key, 2.into()).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    );
}

fn txn_rollback_prewrote<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 1),
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(Default::default()).unwrap();
                let mut txn = mvcc::MvccTxn::new(1.into(), cm.clone());
                let mut reader = SnapshotReader::new(1.into(), snapshot, true);
                cleanup(&mut txn, &mut reader, key, TimeStamp::zero(), false).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_rollback_conflict<E: Engine, F: EngineFactory<E>>(b: &mut Bencher, config: &BenchConfig<F>) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || setup_prewrite(&engine, &config, 2),
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(Default::default()).unwrap();
                let mut txn = mvcc::MvccTxn::new(1.into(), cm.clone());
                let mut reader = SnapshotReader::new(1.into(), snapshot, true);
                cleanup(&mut txn, &mut reader, key, TimeStamp::zero(), false).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

fn txn_rollback_non_prewrote<E: Engine, F: EngineFactory<E>>(
    b: &mut Bencher,
    config: &BenchConfig<F>,
) {
    let engine = config.engine_factory.build();
    let ctx = Context::default();
    let cm = ConcurrencyManager::new(1.into());
    b.iter_batched(
        || {
            let kvs = KvGenerator::new(config.key_length, config.value_length)
                .generate(DEFAULT_ITERATIONS);
            let keys: Vec<Key> = kvs.iter().map(|(k, _)| Key::from_raw(&k)).collect();
            keys
        },
        |keys| {
            for key in keys {
                let snapshot = engine.snapshot(Default::default()).unwrap();
                let mut txn = mvcc::MvccTxn::new(1.into(), cm.clone());
                let mut reader = SnapshotReader::new(1.into(), snapshot, true);
                cleanup(&mut txn, &mut reader, key, TimeStamp::zero(), false).unwrap();
                let write_data = WriteData::from_modifies(txn.into_modifies());
                black_box(engine.write(&ctx, write_data)).unwrap();
            }
        },
        BatchSize::SmallInput,
    )
}

pub fn bench_txn<E: Engine, F: EngineFactory<E>>(c: &mut Criterion, configs: &[BenchConfig<F>]) {
    c.bench_function_over_inputs("txn_prewrite", txn_prewrite, configs.to_owned());
    c.bench_function_over_inputs("txn_commit", txn_commit, configs.to_owned());
    c.bench_function_over_inputs(
        "txn_rollback_prewrote",
        txn_rollback_prewrote,
        configs.to_owned(),
    );
    c.bench_function_over_inputs(
        "txn_rollback_conflict",
        txn_rollback_conflict,
        configs.to_owned(),
    );
    c.bench_function_over_inputs(
        "txn_rollback_non_prewrote",
        txn_rollback_non_prewrote,
        configs.to_owned(),
    );
}
