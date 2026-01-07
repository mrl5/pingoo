use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use rules::RateLimit;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::time::Instant;

pub fn get_rate_limit_handle(mut rx: mpsc::Receiver<Probe>, limiter_cfg: RateLimit) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut limiter = RateLimiter::new(limiter_cfg.max, Duration::from_secs(u64::from(limiter_cfg.window)));
        while let Some(probe) = rx.recv().await {
            let result = limiter.can_resume(probe.ip);
            let _ = probe.resp.send(result);

            limiter.garbage_collect();
        }
    })
}

pub fn get_probe(ip: IpAddr) -> (Probe, oneshot::Receiver<bool>) {
    let (tx, rx) = oneshot::channel();
    (Probe { ip, resp: tx }, rx)
}

type Responder = oneshot::Sender<bool>;
pub struct Probe {
    ip: IpAddr,
    resp: Responder,
}

struct RateLimiter {
    limit: u16,
    sampling_period: Duration,
    current_window: Instant,
    state: HashMap<IpAddr, SlidingWindow>, // todo: from heapless crate
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

impl RateLimiter {
    pub fn new(limit: u16, sampling_period: Duration) -> Self {
        let current_window = Instant::now();
        let mut sanitized_limit = limit;
        if limit == u16::MAX {
            sanitized_limit = limit - 1;
        }

        RateLimiter {
            limit: sanitized_limit,
            sampling_period,
            current_window,
            state: HashMap::new(),
        }
    }

    pub fn can_resume(&mut self, ip: IpAddr) -> bool {
        let now = Instant::now();
        if now >= self.current_window + self.sampling_period {
            self.current_window = now;
        }

        let mut result = false;
        self.state
            .entry(ip)
            .and_modify(|x| result = x.can_resume(self.limit, self.current_window, self.sampling_period))
            .or_insert_with(|| {
                let mut new_ip_state = SlidingWindow::new(self.sampling_period, self.current_window);
                result = new_ip_state.can_resume(self.limit, self.current_window, self.sampling_period);
                new_ip_state
            });
        result
    }

    pub fn garbage_collect(&mut self) {
        const ITEMS: usize = 2;

        let garbage: heapless::Vec<IpAddr, ITEMS> = self
            .state
            .iter()
            .filter(|(_, v)| v.get_last_sample_created_at().elapsed() > 2 * self.sampling_period)
            .take(ITEMS)
            .map(|(k, _)| k.clone())
            .collect();

        for ip in garbage {
            let _ = self.state.remove(&ip);
        }
    }

    pub fn len(&self) -> usize {
        self.state.len()
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
        InMemorySampler { count: 0, starts_at }
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

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use tokio::time::sleep;

    use super::*;

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
        let mut r = RateLimiter::new(limit, sampling_period);
        let ip = Ipv4Addr::new(1, 1, 1, 1).into();

        for _ in 0..limit {
            assert!(r.can_resume(ip), "should allow until limit is not reached");
        }
        for _ in 0..u16::MAX {
            assert!(!r.can_resume(ip), "should break when limit reached");
        }
        sleep(2 * sampling_period).await;

        for _ in 0..42 {
            assert!(r.can_resume(ip), "should resume until limit is not reached");
        }

        sleep(sampling_period + Duration::from_secs(15)).await;
        for _ in 0..19 {
            assert!(r.can_resume(ip), "should resume for 42 * ((60-15)/60) + 19 = 50");
        }

        assert!(!r.can_resume(ip), "should break for 42 * ((60-15)/60) + 20 = 51");

        sleep(Duration::from_secs(3)).await;
        assert!(r.can_resume(ip), "should resume for 42 * ((60-(15+3))/60) + 21 = 50");
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_gc() {
        let sampling_period = Duration::from_secs(60);
        let mut limiter = RateLimiter::new(10, sampling_period);
        let ips = [
            Ipv4Addr::new(1, 1, 1, 1).into(),
            Ipv4Addr::new(2, 2, 2, 2).into(),
            Ipv4Addr::new(3, 3, 3, 3).into(),
            Ipv4Addr::new(4, 4, 4, 4).into(),
            Ipv4Addr::new(5, 5, 5, 5).into(),
        ];
        for ip in ips {
            assert!(limiter.can_resume(ip));
        }

        sleep(sampling_period + Duration::from_secs(1)).await;
        assert!(limiter.can_resume(ips[0]));
        limiter.garbage_collect();
        assert_eq!(ips.len(), limiter.len());

        sleep(sampling_period).await;
        assert!(limiter.can_resume(ips[0]));
        limiter.garbage_collect();
        assert_eq!(
            ips.len() - 2,
            limiter.len(),
            "should garbage collect entries that were not updated for 2 * window, but no more than 2"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_boundary() {
        let ip = Ipv4Addr::new(1, 1, 1, 1).into();
        let mut r = RateLimiter::new(1, Duration::new(1, 0));

        assert!(r.can_resume(ip), "should allow once when limit is 1");
        assert!(!r.can_resume(ip), "should block on 2nd attempt when limit is 1");

        let mut r = RateLimiter::new(0, Duration::new(1, 0));
        assert!(!r.can_resume(ip), "should treat zero limit as always limited");
        assert!(!r.can_resume(ip), "should treat zero limit as always limited");

        let mut r = RateLimiter::new(0, Duration::new(0, 0));
        assert!(
            !r.can_resume(ip),
            "should treat zero limit as always limited, even when zero window"
        );
        assert!(
            !r.can_resume(ip),
            "should treat zero limit as always limited, even when zero window"
        );

        let mut r = RateLimiter::new(1, Duration::new(1, 0));
        assert!(r.can_resume(ip), "allow - limit should take precedense over zero window");
        assert!(!r.can_resume(ip), "block - limit should take precedense over zero window");

        let mut r = RateLimiter::new(u16::MAX, Duration::new(1, 0));
        for _ in 1..u16::MAX {
            assert!(r.can_resume(ip), "allow - should handle limit overflow");
        }
        assert!(!r.can_resume(ip), "block - should handle limit overflow");
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
