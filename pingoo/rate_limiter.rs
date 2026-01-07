use std::net::IpAddr;
use std::sync::Arc;

use heapless::index_map::Entry;
use heapless::index_map::FnvIndexMap;
use heapless::index_map::Iter;
use rules::RateLimit;
use rules::RateLimitBucketSize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::time::Instant;

pub fn get_rate_limit_handle(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    match limiter_cfg.capacity {
        RateLimitBucketSize::Bucket8 => get_rate_limit_handle_b8(rx, limiter_cfg),
        RateLimitBucketSize::Bucket9 => get_rate_limit_handle_b9(rx, limiter_cfg),
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
    inner: FnvIndexMap<IpAddr, SlidingWindow, N>,
}
struct RateLimiter<const N: usize> {
    limit: u16,
    sampling_period: Duration,
    current_window: Instant,
    bucket: RateLimiterBucket<N>,
}

struct SlidingWindow {
    sampler_green: InMemorySampler,
    sampler_blue: InMemorySampler,
    curr_sampler: Arc<InMemorySampler>,
    prev_sampler: Arc<InMemorySampler>,
}

trait Sampler {
    fn new(starts_at: Instant) -> Self;
    fn increment(&mut self);
    fn reset(&mut self, starts_at: Instant);
    fn get_count(&self) -> u16;
    fn get_starts_at(&self) -> Instant;
    fn get_approx(&self, sampling_period: Duration, next_window_needle: Duration) -> u64;
}

#[derive(Debug, Copy, Clone)]
struct InMemorySampler {
    count: u16,
    starts_at: Instant,
}

impl<const N: usize> RateLimiterBucket<N> {
    pub fn new() -> Self {
        Self {
            inner: FnvIndexMap::new(),
        }
    }

    pub fn entry(&mut self, key: IpAddr) -> Entry<'_, IpAddr, SlidingWindow, N> {
        self.inner.entry(key)
    }

    pub fn iter(&self) -> Iter<'_, IpAddr, SlidingWindow> {
        self.inner.iter()
    }

    pub fn remove(&mut self, key: &IpAddr) -> Option<SlidingWindow> {
        self.inner.remove(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<const N: usize> RateLimiter<N> {
    pub fn new(limit: u16, sampling_period: Duration) -> Self {
        let current_window = Instant::now();
        let mut sanitized_limit = limit;
        if limit == u16::MAX {
            sanitized_limit = limit - 1;
        }

        Self {
            limit: sanitized_limit,
            sampling_period,
            current_window,
            bucket: RateLimiterBucket::new(),
        }
    }

    pub fn can_resume(&mut self, ip: IpAddr) -> Result<bool, ()> {
        let now = Instant::now();
        if now >= self.current_window + self.sampling_period {
            self.current_window = now;
        }

        let mut can_resume = false;
        if let Ok(_) = self
            .bucket
            .entry(ip)
            .and_modify(|x| can_resume = x.can_resume(self.limit, self.current_window, self.sampling_period))
            .or_insert_with(|| {
                let mut new_ip_bucket = SlidingWindow::new(self.sampling_period, self.current_window);
                can_resume = new_ip_bucket.can_resume(self.limit, self.current_window, self.sampling_period);
                new_ip_bucket
            })
        {
            return Ok(can_resume);
        }

        Err(())
    }

    pub fn garbage_collect(&mut self) {
        const ITEMS: usize = 2;

        let garbage: heapless::Vec<IpAddr, ITEMS> = self
            .bucket
            .iter()
            .filter(|(_, v)| v.get_last_sample_created_at().elapsed() > 2 * self.sampling_period)
            .take(ITEMS)
            .map(|(k, _)| k.clone())
            .collect();

        for ip in garbage {
            let _ = self.bucket.remove(&ip);
        }
    }

    pub fn len(&self) -> usize {
        self.bucket.len()
    }
}

impl SlidingWindow {
    pub fn new(sampling_period: Duration, current_window: Instant) -> Self {
        let prev_window = current_window - sampling_period;

        let curr = InMemorySampler::new(current_window);
        let prev = InMemorySampler::new(prev_window);
        SlidingWindow {
            sampler_green: prev,
            sampler_blue: curr,
            curr_sampler: Arc::new(curr),
            prev_sampler: Arc::new(prev),
        }
    }

    pub fn can_resume(&mut self, limit: u16, current_window: Instant, sampling_period: Duration) -> bool {
        if limit == 0 {
            return false;
        }

        if current_window != self.curr_sampler.get_starts_at() {
            self.shuffle_samplers(current_window, sampling_period);
        }

        Arc::make_mut(&mut self.curr_sampler).increment();

        let approx = self
            .prev_sampler
            .get_approx(sampling_period, self.prev_sampler.get_starts_at().elapsed() - sampling_period);
        let current_count = self.curr_sampler.get_count();

        u64::from(limit) >= approx + u64::from(current_count)
    }

    pub fn get_last_sample_created_at(&self) -> Instant {
        self.curr_sampler.starts_at
    }

    fn shuffle_samplers(&mut self, current_window: Instant, sampling_period: Duration) {
        let mut next_sampler = self.prev_sampler.clone();
        Arc::make_mut(&mut next_sampler).reset(current_window);

        if current_window.elapsed() > sampling_period + self.curr_sampler.get_starts_at().elapsed() {
            Arc::make_mut(&mut self.curr_sampler).reset(current_window - sampling_period);
        }

        self.prev_sampler = self.curr_sampler.clone();
        self.curr_sampler = next_sampler;
    }
}

impl Sampler for InMemorySampler {
    fn new(starts_at: Instant) -> Self {
        Self { count: 0, starts_at }
    }

    fn increment(&mut self) {
        self.count = self.count.saturating_add(1);
    }

    fn reset(&mut self, starts_at: Instant) {
        self.count = 0;
        self.starts_at = starts_at;
    }

    fn get_count(&self) -> u16 {
        self.count
    }

    fn get_starts_at(&self) -> Instant {
        self.starts_at
    }

    fn get_approx(&self, sampling_period: Duration, next_window_needle: Duration) -> u64 {
        if next_window_needle >= sampling_period {
            return 0;
        }

        u64::from(self.count) * (sampling_period.as_secs() - next_window_needle.as_secs()) / sampling_period.as_secs()
    }
}

// todo: some makro/crate to avoid this ugly pattern, which is a consequence of using heapless::index_map::FnvIndexMap
// todo: alternatively decide which buckets we want to support -- for reference see test_memory_footprint()
fn get_rate_limit_handle_b8(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(8) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);

            limiter.garbage_collect();
        }
    })
}
fn get_rate_limit_handle_b9(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    let mut limiter =
        RateLimiter::<{ 2usize.pow(9) }>::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
    tokio::spawn(async move {
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);

            limiter.garbage_collect();
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
    fn test_memory_footprint() {
        assert_eq!(17, std::mem::size_of::<IpAddr>());
        assert_eq!(64, std::mem::size_of::<SlidingWindow>());

        assert_eq!(96_469_040, std::mem::size_of::<RateLimiter<{ 2usize.pow(20) }>>()); // ~one milion IPs -> 96 MB of mem footprint

        assert_eq!(23_600, std::mem::size_of::<RateLimiter<256>>()); // bucket_8 can store 256 IPs and consume 23.6 kB
        assert_eq!(94_256, std::mem::size_of::<RateLimiter<1_024>>()); // bucket_10 -> 94 kB
        assert_eq!(1_507_376, std::mem::size_of::<RateLimiter<{ 2usize.pow(14) }>>()); // bucket_14 -> 16 384 IPs -> 1.5 MB
        assert_eq!(6_029_360, std::mem::size_of::<RateLimiter<{ 2usize.pow(16) }>>()); // 65 536 IPs -> 6 MB
        assert_eq!(12_058_672, std::mem::size_of::<RateLimiter<{ 2usize.pow(17) }>>()); // 131 072 IPs -> 12 MB
        assert_eq!(48_234_544, std::mem::size_of::<RateLimiter<{ 2usize.pow(19) }>>()); // 524 288 IPs -> 48 MB
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

        for _ in 0..limit {
            assert!(r.can_resume(ip).unwrap(), "should allow until limit is not reached");
        }
        for _ in 0..u16::MAX {
            assert!(!r.can_resume(ip).unwrap(), "should break when limit reached");
        }
        sleep(2 * sampling_period).await;

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
    }

    // todo: test backpressure behavior

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_gc() {
        let sampling_period = Duration::from_secs(60);
        let mut limiter = RateLimiter::<8>::new(10, sampling_period);
        let ips = [
            Ipv4Addr::new(1, 1, 1, 1).into(),
            Ipv4Addr::new(2, 2, 2, 2).into(),
            Ipv4Addr::new(3, 3, 3, 3).into(),
            Ipv4Addr::new(4, 4, 4, 4).into(),
            Ipv4Addr::new(5, 5, 5, 5).into(),
        ];
        for ip in ips {
            assert!(limiter.can_resume(ip).unwrap());
        }

        sleep(sampling_period + Duration::from_secs(1)).await;
        assert!(limiter.can_resume(ips[0]).unwrap());
        limiter.garbage_collect();
        assert_eq!(ips.len(), limiter.len());

        sleep(sampling_period).await;
        assert!(limiter.can_resume(ips[0]).unwrap());
        limiter.garbage_collect();
        assert_eq!(
            ips.len() - 2,
            limiter.len(),
            "should garbage collect up to 2 entries that were not updated for 2 * window"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_boundary() {
        let ips = [
            Ipv4Addr::new(1, 1, 1, 1).into(),
            Ipv4Addr::new(2, 2, 2, 2).into(),
            Ipv4Addr::new(3, 3, 3, 3).into(),
        ];
        let ip = ips[0];
        let mut r = RateLimiter::<2>::new(1, Duration::new(1, 0));

        assert!(r.can_resume(ip).unwrap(), "should allow once when limit is 1");
        assert!(!r.can_resume(ip).unwrap(), "should block on 2nd attempt when limit is 1");

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

        let mut r = RateLimiter::<2>::new(1, Duration::new(1, 0));
        assert!(r.can_resume(ips[0]).unwrap(), "allow - should handle this IP");
        assert!(r.can_resume(ips[1]).unwrap(), "allow - should handle that IP");
        assert!(r.can_resume(ips[2]).is_err(), "error - should backpressure on another IP");
    }

    #[tokio::test(start_paused = true)]
    async fn test_inmemory_get_approx() {
        let sampling_period = Duration::from_secs(60);
        let mut r = InMemorySampler::new(Instant::now());
        sleep(sampling_period).await;
        for _ in 0..42 {
            r.increment();
        }

        let start = Instant::now();
        assert_eq!(42, r.get_count());
        assert_eq!(42, r.get_approx(sampling_period, start.elapsed(),));
        sleep(Duration::from_secs(15)).await;
        assert_eq!(31, r.get_approx(sampling_period, start.elapsed(),));
    }
}
