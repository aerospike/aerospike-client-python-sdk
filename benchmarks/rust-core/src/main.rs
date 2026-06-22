//! Minimal Rust-core bench — same workload shape as the Python minimal harnesses.
//!
//! Honors env vars: AEROSPIKE_HOST, NAMESPACE, SET, KEYS, THREADS (sync mode) or
//! TASKS (async mode), DURATION, WARMUP, READ_PCT, LAT_SAMPLE_EVERY, MODE.
//! MODE defaults to "async". One shared Client across all workers, tight loop
//! with deadline, sampled per-op latency (1-in-N timed).

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerospike_core::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let host = env::var("AEROSPIKE_HOST").unwrap_or_else(|_| "10.138.0.3:3100".into());
    let ns = env::var("NAMESPACE").unwrap_or_else(|_| "test".into());
    let set = env::var("SET").unwrap_or_else(|_| "test".into());
    let keys: i32 = env::var("KEYS").unwrap_or_else(|_| "100000".into()).parse().unwrap();
    let workers: usize = env::var("THREADS")
        .or_else(|_| env::var("TASKS"))
        .unwrap_or_else(|_| "32".into())
        .parse()
        .unwrap();
    let duration: f64 = env::var("DURATION").unwrap_or_else(|_| "15".into()).parse().unwrap();
    let warmup: f64 = env::var("WARMUP").unwrap_or_else(|_| "3".into()).parse().unwrap();
    let read_pct: i32 = env::var("READ_PCT").unwrap_or_else(|_| "50".into()).parse().unwrap();
    let lat_sample_every: u64 = env::var("LAT_SAMPLE_EVERY")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap();
    let mode = env::var("MODE").unwrap_or_else(|_| "async".into());

    let mut policy = ClientPolicy::default();
    // Honor AEROSPIKE_USE_SERVICES_ALTERNATE (truthy = true). Default false —
    // bench-asd publishes its real addresses via the standard services list,
    // and turning on services-alternate routes through unreachable addresses,
    // producing partition-map errors that the bench would otherwise count as
    // successful attempts.
    policy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    // Connection pools per node. aerospike-core defaults to 1, which on
    // <=8-core hosts serializes every checkout/return on a single per-node
    // Mutex<VecDeque<Connection>> — visible as a throughput cap at high
    // concurrency. Mirrors the official benchmark's --connPoolsPerNode knob so
    // the contention can be measured rather than assumed.
    policy.conn_pools_per_node = env::var("CONN_POOLS_PER_NODE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    // Max connections per node. Default 256 fails-fast on burst > capacity at
    // high concurrency (see Phase 4b in the timer-churn plan). Lifting it tests
    // whether a high-concurrency cell is pool-bound vs cluster-bound, without
    // adding throttling.
    if let Some(n) = env::var("MAX_CONNS_PER_NODE").ok().and_then(|v| v.parse().ok()) {
        policy.max_conns_per_node = n;
    }
    let client = Arc::new(Client::new(&policy, &host).await.expect("connect"));

    // Phase 4 timeout guards: drive total_timeout / socket_timeout from env.
    // TOTAL_TIMEOUT_MS=1 exercises the deadline()-clamp path (budget exhausted
    // before/during IO → zero-duration Sleep → immediate Err(Timeout), no panic).
    let mut read_policy = ReadPolicy::default();
    let mut write_policy = WritePolicy::default();
    if let Some(n) = env::var("TOTAL_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()) {
        read_policy.base_policy.total_timeout = n;
        write_policy.base_policy.total_timeout = n;
    }
    if let Some(n) = env::var("SOCKET_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()) {
        read_policy.base_policy.socket_timeout = n;
        write_policy.base_policy.socket_timeout = n;
    }
    let read_policy = Arc::new(read_policy);
    let write_policy = Arc::new(write_policy);

    println!(
        "rust mode={} workers={} duration={}s warmup={}s read_pct={} conn_pools_per_node={} max_conns_per_node={} total_timeout={} socket_timeout={}",
        mode, workers, duration, warmup, read_pct, policy.conn_pools_per_node, policy.max_conns_per_node,
        read_policy.base_policy.total_timeout, read_policy.base_policy.socket_timeout
    );

    if mode == "sync" {
        run_sync(
            client, ns, set, keys, workers, duration, warmup, read_pct,
            lat_sample_every, read_policy, write_policy,
        )
        .await;
    } else {
        run_async(
            client, ns, set, keys, workers, duration, warmup, read_pct,
            lat_sample_every, read_policy, write_policy,
        )
        .await;
    }
}

/// Async mode: one Tokio runtime, N concurrent tasks (spawned). Closest to
/// "natural" Rust-client usage.
async fn run_async(
    client: Arc<Client>,
    ns: String,
    set: String,
    keys: i32,
    tasks: usize,
    duration: f64,
    warmup: f64,
    read_pct: i32,
    lat_sample_every: u64,
    read_policy: Arc<ReadPolicy>,
    write_policy: Arc<WritePolicy>,
) {
    // Warmup
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(warmup);
    let mut handles = Vec::with_capacity(tasks);
    for tid in 0..tasks {
        let client = client.clone();
        let ns = ns.clone();
        let set = set.clone();
        let rp = read_policy.clone();
        let wp = write_policy.clone();
        handles.push(tokio::spawn(async move {
            worker_async(tid, client, ns, set, keys, warmup_deadline, read_pct, 0, rp, wp).await
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    // Measured
    let t0 = Instant::now();
    let deadline = Instant::now() + Duration::from_secs_f64(duration);
    let mut handles = Vec::with_capacity(tasks);
    for tid in 0..tasks {
        let client = client.clone();
        let ns = ns.clone();
        let set = set.clone();
        let rp = read_policy.clone();
        let wp = write_policy.clone();
        handles.push(tokio::spawn(async move {
            worker_async(
                tid, client, ns, set, keys, deadline, read_pct, lat_sample_every, rp, wp,
            )
            .await
        }));
    }
    let mut total_ok: u64 = 0;
    let mut total_err: u64 = 0;
    let mut all_lat: Vec<Duration> = Vec::new();
    for h in handles {
        if let Ok((ok, err, lats)) = h.await {
            total_ok += ok;
            total_err += err;
            all_lat.extend(lats);
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    report(total_ok, total_err, duration, elapsed, &mut all_lat);
}

async fn worker_async(
    tid: usize,
    client: Arc<Client>,
    ns: String,
    set: String,
    keys: i32,
    deadline: Instant,
    read_pct: i32,
    lat_sample_every: u64,
    read_policy: Arc<ReadPolicy>,
    write_policy: Arc<WritePolicy>,
) -> (u64, u64, Vec<Duration>) {
    let mut rng = SmallRng::seed_from_u64(tid as u64 + 1);
    let mut ok: u64 = 0;
    let mut err: u64 = 0;
    let mut lat: Vec<Duration> = Vec::new();
    while Instant::now() < deadline {
        let kid = rng.gen_range(0..keys);
        let key = as_key!(ns.as_str(), set.as_str(), kid);
        let total = ok + err;
        let sample = lat_sample_every > 0 && total % lat_sample_every == 0;
        let t_op = if sample { Some(Instant::now()) } else { None };
        if rng.gen_range(0..100) < read_pct {
            match client.get(&read_policy, &key, Bins::All).await {
                Ok(_) => ok += 1,
                Err(_) => err += 1,
            }
        } else {
            let bin = as_bin!("b0", kid as i64);
            match client.put(&write_policy, &key, &[bin]).await {
                Ok(_) => ok += 1,
                Err(_) => err += 1,
            }
        }
        if let Some(t) = t_op {
            lat.push(t.elapsed());
        }
    }
    (ok, err, lat)
}

/// Sync mode: N OS threads, each calling client.get/put via the shared
/// Tokio runtime's `Handle::block_on`. Closest to the PAC `pac-blocking`
/// pattern (sync wrapper around an async core).
async fn run_sync(
    client: Arc<Client>,
    ns: String,
    set: String,
    keys: i32,
    workers: usize,
    duration: f64,
    warmup: f64,
    read_pct: i32,
    lat_sample_every: u64,
    read_policy: Arc<ReadPolicy>,
    write_policy: Arc<WritePolicy>,
) {
    let handle = tokio::runtime::Handle::current();

    // Warmup
    let warmup_deadline = Instant::now() + Duration::from_secs_f64(warmup);
    let mut threads = Vec::with_capacity(workers);
    for tid in 0..workers {
        let client = client.clone();
        let ns = ns.clone();
        let set = set.clone();
        let rp = read_policy.clone();
        let wp = write_policy.clone();
        let h = handle.clone();
        threads.push(std::thread::spawn(move || {
            worker_sync(tid, client, ns, set, keys, warmup_deadline, read_pct, 0, rp, wp, h)
        }));
    }
    for t in threads {
        let _ = t.join();
    }

    // Measured
    let t0 = Instant::now();
    let deadline = Instant::now() + Duration::from_secs_f64(duration);
    let mut threads = Vec::with_capacity(workers);
    for tid in 0..workers {
        let client = client.clone();
        let ns = ns.clone();
        let set = set.clone();
        let rp = read_policy.clone();
        let wp = write_policy.clone();
        let h = handle.clone();
        threads.push(std::thread::spawn(move || {
            worker_sync(
                tid, client, ns, set, keys, deadline, read_pct, lat_sample_every, rp, wp, h,
            )
        }));
    }
    let mut total_ok: u64 = 0;
    let mut total_err: u64 = 0;
    let mut all_lat: Vec<Duration> = Vec::new();
    for t in threads {
        if let Ok((ok, err, lats)) = t.join() {
            total_ok += ok;
            total_err += err;
            all_lat.extend(lats);
        }
    }
    let elapsed = t0.elapsed().as_secs_f64();
    report(total_ok, total_err, duration, elapsed, &mut all_lat);
}

fn worker_sync(
    tid: usize,
    client: Arc<Client>,
    ns: String,
    set: String,
    keys: i32,
    deadline: Instant,
    read_pct: i32,
    lat_sample_every: u64,
    read_policy: Arc<ReadPolicy>,
    write_policy: Arc<WritePolicy>,
    handle: tokio::runtime::Handle,
) -> (u64, u64, Vec<Duration>) {
    let mut rng = SmallRng::seed_from_u64(tid as u64 + 1);
    let mut ok: u64 = 0;
    let mut err: u64 = 0;
    let mut lat: Vec<Duration> = Vec::new();
    while Instant::now() < deadline {
        let kid = rng.gen_range(0..keys);
        let key = as_key!(ns.as_str(), set.as_str(), kid);
        let total = ok + err;
        let sample = lat_sample_every > 0 && total % lat_sample_every == 0;
        let t_op = if sample { Some(Instant::now()) } else { None };
        if rng.gen_range(0..100) < read_pct {
            match handle.block_on(client.get(&read_policy, &key, Bins::All)) {
                Ok(_) => ok += 1,
                Err(_) => err += 1,
            }
        } else {
            let bin = as_bin!("b0", kid as i64);
            match handle.block_on(client.put(&write_policy, &key, &[bin])) {
                Ok(_) => ok += 1,
                Err(_) => err += 1,
            }
        }
        if let Some(t) = t_op {
            lat.push(t.elapsed());
        }
    }
    (ok, err, lat)
}

fn report(ok: u64, err: u64, requested_duration: f64, wall: f64, lat: &mut [Duration]) {
    lat.sort();
    let pct = |p: f64| -> u128 {
        if lat.is_empty() {
            0
        } else {
            let i = ((lat.len() as f64 * p / 100.0) as usize).min(lat.len() - 1);
            lat[i].as_micros()
        }
    };
    let total = ok + err;
    let tps = (ok as f64 / requested_duration).round() as u64;
    let attempt_tps = (total as f64 / requested_duration).round() as u64;
    let err_pct = if total > 0 { 100.0 * err as f64 / total as f64 } else { 0.0 };
    println!(
        "measured: ok={}, err={} ({:.2}% of attempts), attempts={}, attempt_TPS={}, ok_TPS={}, duration={}s, wall={:.1}s, lat_samples={}, p50={}us, p99={}us, p999={}us",
        ok, err, err_pct,
        total,
        attempt_tps,
        tps,
        requested_duration,
        wall,
        lat.len(),
        pct(50.0),
        pct(99.0),
        pct(99.9),
    );
}
