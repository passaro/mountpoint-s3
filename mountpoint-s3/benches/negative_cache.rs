use criterion::{criterion_group, criterion_main, Criterion};
use mountpoint_s3::inode::NegativeCache;
use rand::distributions::Alphanumeric;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::thread::{self};
use std::time::{Duration, Instant};

pub fn insert(c: &mut Criterion) {
    let names = random_names();

    c.bench_function("insert", |b| {
        b.iter(|| {
            let cache = NegativeCache::new(100000, Duration::from_secs(3600));
            for name in &names {
                cache.insert(20, name);
            }
        })
    });
}

pub fn insert_evict(c: &mut Criterion) {
    let names = random_names();

    c.bench_function("insert_evict", |b| {
        b.iter_batched(
            || {
                let cache = NegativeCache::new(100000, Duration::from_millis(500));

                let start = Instant::now();
                for name in &names {
                    cache.insert(20, name);
                }

                assert!(
                    start.elapsed() < Duration::from_millis(500),
                    "elapsed: {:#?}",
                    start.elapsed()
                );
                thread::sleep(Duration::from_secs(1));
                cache
            },
            |cache| {
                cache.insert(42, "unique");
                assert_eq!(cache.len(), 1);
            },
            criterion::BatchSize::PerIteration,
        )
    });
}

fn random_names() -> Vec<String> {
    let mut rng = ChaCha20Rng::seed_from_u64(123);
    let length = rng.gen_range(1..1000);
    let names: Vec<String> = (0..100_000)
        .map(|_| {
            (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(length)
                .map(char::from)
                .collect()
        })
        .collect();
    names
}

criterion_group!(benches, insert, insert_evict);
criterion_main!(benches);
