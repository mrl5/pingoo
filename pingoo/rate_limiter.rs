use heapless::index_map::FnvIndexMap;
use rules::RateLimit;
use rules::RateLimitBucketSize;
use std::net::IpAddr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::time::Instant;

pub fn get_rate_limit_handle(rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    match limiter_cfg.capacity {
        // duplicated logic in each function is a consequence of using heapless::index_map::FnvIndexMap
        // the only difference between them is Map capacity
        // todo: some cleaner solution -- macro maybe?
        RateLimitBucketSize::Bucket10 => get_rate_limit_handle_b10(rx, limiter_cfg),
        RateLimitBucketSize::Bucket14 => get_rate_limit_handle_b14(rx, limiter_cfg),
        RateLimitBucketSize::Bucket16 => get_rate_limit_handle_b16(rx, limiter_cfg),
        RateLimitBucketSize::Bucket17 => get_rate_limit_handle_b17(rx, limiter_cfg),
        RateLimitBucketSize::Bucket19 => get_rate_limit_handle_b19(rx, limiter_cfg),
        RateLimitBucketSize::Bucket20 => get_rate_limit_handle_b20(rx, limiter_cfg),
        RateLimitBucketSize::Bucket23 => get_rate_limit_handle_b23(rx, limiter_cfg),
        RateLimitBucketSize::Bucket24 => get_rate_limit_handle_b24(rx, limiter_cfg),
    }
}

pub fn get_probe(ip: IpAddr) -> (Probe, oneshot::Receiver<Response>) {
    let (tx, rx) = oneshot::channel();
    (Probe { ip, resp: tx }, rx)
}

type Response = Result<bool, ()>;
type Responder = oneshot::Sender<Response>;
pub struct Probe {
    ip: IpAddr,
    resp: Responder,
}

struct RateLimiterBucket<const N: usize> {
    starts_at: Instant,
    inner: FnvIndexMap<IpAddr, Counter, N>,
}
struct RateLimiter<const N: usize> {
    limit: u16,
    sampling_period: Duration,
    bucket_green: RateLimiterBucket<N>,
    bucket_blue: RateLimiterBucket<N>,
}

#[derive(Debug)]
struct Counter {
    pub sum: u16,
}

impl<const N: usize> RateLimiterBucket<N> {
    pub fn new(starts_at: Instant) -> Self {
        Self {
            starts_at,
            inner: FnvIndexMap::new(),
        }
    }
}

impl<const N: usize> RateLimiter<N> {
    pub fn new(limit: u16, sampling_period: Duration) -> Self {
        let mut sanitized_limit = limit;
        if limit == u16::MAX {
            sanitized_limit = limit - 1;
        }

        let now = Instant::now();
        let before = create_prev_window(now, sampling_period);

        let bucket_green = RateLimiterBucket::new(now);
        let bucket_blue = RateLimiterBucket::new(before);

        Self {
            limit: sanitized_limit,
            sampling_period,
            bucket_green,
            bucket_blue,
        }
    }

    pub fn can_resume(&mut self, ip: IpAddr) -> Result<bool, ()> {
        if self.limit == 0 {
            return Ok(false);
        }

        let is_green_bucket_current = self.bucket_green.starts_at >= self.bucket_blue.starts_at;
        let starts_at = match is_green_bucket_current {
            true => self.bucket_green.starts_at,
            false => self.bucket_blue.starts_at,
        };
        let now = Instant::now();
        if !self.is_within_curr_bucket_window(now, starts_at) && self.sampling_period > Duration::from_nanos(0) {
            if self.is_outside_next_monothonic_window(now, self.bucket_green.starts_at) {
                self.bucket_green = RateLimiterBucket::new(now);
                self.bucket_blue = RateLimiterBucket::new(now);
            } else {
                // current bucket becomes previous
                if is_green_bucket_current {
                    let starts_at = self.bucket_green.starts_at + self.sampling_period;
                    self.bucket_blue = RateLimiterBucket::new(starts_at);
                } else {
                    let starts_at = self.bucket_blue.starts_at + self.sampling_period;
                    self.bucket_green = RateLimiterBucket::new(starts_at);
                }
            }
        }

        let is_green_bucket_current = self.bucket_green.starts_at >= self.bucket_blue.starts_at;
        let (curr_bucket, prev_bucket) = match is_green_bucket_current {
            true => (&mut self.bucket_green, &mut self.bucket_blue),
            false => (&mut self.bucket_blue, &mut self.bucket_green),
        };

        let curr_counter = curr_bucket
            .inner
            .entry(ip)
            .and_modify(|x| {
                x.increment();
            })
            .or_insert_with(|| {
                let mut x = Counter::new();
                x.increment();
                x
            });
        if curr_counter.is_err() {
            return Err(());
        }
        let curr_sum = curr_counter.expect("counter should exist").sum;

        let prev_sum = match prev_bucket.inner.get(&ip) {
            Some(c) => c.sum,
            None => 0,
        };

        let approx = get_approx(prev_sum, curr_bucket.starts_at.elapsed(), self.sampling_period);
        Ok(u64::from(self.limit) >= approx + u64::from(curr_sum))
    }

    fn is_within_curr_bucket_window(&self, now: Instant, curr_starts_at: Instant) -> bool {
        let next_monothonic_window = curr_starts_at + self.sampling_period;
        now < next_monothonic_window
    }

    fn is_outside_next_monothonic_window(&self, now: Instant, curr_starts_at: Instant) -> bool {
        let next_monothonic_window = curr_starts_at + 2 * self.sampling_period;
        now >= next_monothonic_window
    }
}

impl Counter {
    pub fn new() -> Self {
        Self { sum: 0 }
    }

    pub fn increment(&mut self) {
        self.sum = self.sum.saturating_add(1);
    }
}

fn get_approx(prev_counter: u16, window_needle: Duration, sampling_period: Duration) -> u64 {
    if window_needle >= sampling_period {
        return 0;
    }

    u64::from(prev_counter) * (sampling_period.as_secs() - window_needle.as_secs()) / sampling_period.as_secs()
}

fn create_prev_window(instant: Instant, sampling_period: Duration) -> Instant {
    if instant.elapsed() < sampling_period {
        return instant;
    }
    instant - sampling_period
}

fn get_rate_limit_handle_b10(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(10) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b14(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(14) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b16(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(16) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b17(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(17) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b19(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(19) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b20(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(20) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b23(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(23) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}
fn get_rate_limit_handle_b24(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(24) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);
        }
    })
}

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use tokio::time::sleep;

    use super::*;

    #[test]
    // this test case serves more for memory footprint documentation
    fn test_memory_footprint() {
        assert_eq!(17, std::mem::size_of::<IpAddr>());
        assert_eq!(2, std::mem::size_of::<Counter>());

        assert_eq!(54_526_024, std::mem::size_of::<RateLimiter<{ 2usize.pow(20) }>>()); // ~million IPs -> 54.5 MB of mem footprint

        assert_eq!(53_320, std::mem::size_of::<RateLimiter<1_024>>()); // bucket_10 can store 1024 IPs and consume 53 kB
        assert_eq!(852_040, std::mem::size_of::<RateLimiter<{ 2usize.pow(14) }>>()); // bucket_14 -> 16 384 IPs -> 852 kB
        assert_eq!(3_407_944, std::mem::size_of::<RateLimiter<{ 2usize.pow(16) }>>()); // 65 536 IPs -> 3.4 MB
        assert_eq!(6_815_816, std::mem::size_of::<RateLimiter<{ 2usize.pow(17) }>>()); // 131 072 IPs -> 6.8 MB
        assert_eq!(27_263_048, std::mem::size_of::<RateLimiter<{ 2usize.pow(19) }>>()); // 524 288 IPs -> 27.2 MB
        assert_eq!(436_207_688, std::mem::size_of::<RateLimiter<{ 2usize.pow(23) }>>()); // ~9 million IPs -> 436.2 MB
        assert_eq!(872_415_304, std::mem::size_of::<RateLimiter<{ 2usize.pow(24) }>>()); // ~17 million IPs -> 872.4 MB
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_alg() {
        // Tests example form https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
        //
        // "Let's say I set a limit of 50 requests per minute on an API endpoint.
        // In this situation, I did 18 requests during the current minute, which started 15 seconds ago
        // and 42 requests during the entire previous minute."
        //
        // rate = 42 * ((60-15)/60) + 18
        //      = 42 * 0.75 + 18
        //      = 49.5 requests
        let limit = 50;
        let sampling_period = Duration::from_secs(60);
        let mut r = RateLimiter::<2>::new(limit, sampling_period);
        let ip = Ipv4Addr::new(1, 1, 1, 1).into();

        for _ in 0..42 {
            assert!(r.can_resume(ip).unwrap(), "should resume until limit is not reached");
        }

        sleep(sampling_period + Duration::from_secs(15)).await;
        for _ in 0..19 {
            assert!(r.can_resume(ip).unwrap(), "should resume for 42 * ((60-15)/60) + 19 = 50");
        }

        assert!(!r.can_resume(ip).unwrap(), "should break for 42 * ((60-15)/60) + 20 = 51");

        sleep(Duration::from_secs(3)).await;
        assert!(r.can_resume(ip).unwrap(), "should resume for 42 * ((60-(15+3))/60) + 21 = 50");

        sleep(2 * sampling_period).await;
        for _ in 0..limit {
            assert!(r.can_resume(ip).unwrap(), "should allow until limit is not reached");
        }
        for _ in 0..u16::MAX {
            assert!(!r.can_resume(ip).unwrap(), "should break when limit reached");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_backpressure() {
        let sampling_period = Duration::new(1, 0);
        let ips = [
            Ipv4Addr::new(1, 1, 1, 1).into(),
            Ipv4Addr::new(2, 2, 2, 2).into(),
            Ipv4Addr::new(3, 3, 3, 3).into(),
            Ipv4Addr::new(4, 4, 4, 4).into(),
        ];
        let mut r = RateLimiter::<2>::new(10, sampling_period);
        assert!(r.can_resume(ips[0]).unwrap(), "allow - should handle this IP");
        assert!(r.can_resume(ips[1]).unwrap(), "allow - should handle that IP");
        assert!(r.can_resume(ips[2]).is_err(), "error - should backpressure on another IP");
        assert!(r.can_resume(ips[0]).unwrap(), "allow - should still handle this IP");
        assert!(r.can_resume(ips[1]).unwrap(), "allow - should still handle that IP");
        assert!(r.can_resume(ips[2]).is_err(), "error - should again backpressure on another IP");

        sleep(sampling_period).await;
        assert!(r.can_resume(ips[2]).unwrap(), "allow - another IP after bucket rotation");
        assert!(r.can_resume(ips[3]).unwrap(), "allow - new IP after bucket rotation");
        assert!(r.can_resume(ips[0]).is_err(), "error - should backpressure on this IP");
        assert!(r.can_resume(ips[1]).is_err(), "error - should backpressure on that IP");
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_boundary_single_ip() {
        let ip = Ipv4Addr::new(1, 1, 1, 1).into();
        let mut r = RateLimiter::<2>::new(1, Duration::new(1, 0));

        assert!(r.can_resume(ip).unwrap(), "should allow once when limit is 1");
        assert!(!r.can_resume(ip).unwrap(), "should block n 2nd attempt when limit is 1");

        let mut r = RateLimiter::<2>::new(0, Duration::new(1, 0));
        assert!(!r.can_resume(ip).unwrap(), "should treat zero limit as always limited");
        assert!(!r.can_resume(ip).unwrap(), "should treat zero limit as always limited");

        let mut r = RateLimiter::<2>::new(0, Duration::new(0, 0));
        assert!(
            !r.can_resume(ip).unwrap(),
            "should treat zero limit as always limited, even when zero window"
        );
        assert!(
            !r.can_resume(ip).unwrap(),
            "should treat zero limit as always limited, even when zero window"
        );

        let mut r = RateLimiter::<2>::new(1, Duration::new(0, 0));
        assert!(
            r.can_resume(ip).unwrap(),
            "allow - limit should take precedense over zero window"
        );
        assert!(
            !r.can_resume(ip).unwrap(),
            "block - limit should take precedense over zero window"
        );

        let mut r = RateLimiter::<2>::new(u16::MAX, Duration::new(1, 0));
        for _ in 1..u16::MAX {
            assert!(r.can_resume(ip).unwrap(), "allow - should handle limit overflow");
        }
        assert!(!r.can_resume(ip).unwrap(), "block - should handle limit overflow");
    }

    #[tokio::test(start_paused = true)]
    async fn test_inmemory_get_approx() {
        let sampling_period = Duration::from_secs(60);
        let mut counter: u16 = 0;
        sleep(sampling_period).await;
        for _ in 0..42 {
            counter = counter.saturating_add(1);
        }

        let start = Instant::now();
        assert_eq!(42, counter);
        assert_eq!(42, get_approx(counter, start.elapsed(), sampling_period));
        sleep(Duration::from_secs(15)).await;
        assert_eq!(31, get_approx(counter, start.elapsed(), sampling_period));
    }

    #[tokio::test(start_paused = true)]
    async fn test_create_prev_window() {
        let sampling_window = Duration::from_secs(60);

        for tc in vec![0, 13, 59] {
            let now = Instant::now();
            let offset = Duration::from_secs(tc);
            sleep(offset).await;
            assert_eq!(now, create_prev_window(now, sampling_window));
        }

        for tc in vec![60, 73, 119] {
            let now = Instant::now();
            let offset = Duration::from_secs(tc);
            sleep(offset).await;
            assert_eq!(now - sampling_window, create_prev_window(now, sampling_window));
        }
    }
}
