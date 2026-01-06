use std::collections::HashMap;
use std::net::IpAddr;

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
    window: Duration,
    state: HashMap<IpAddr, SlidingWindow>,
}

struct SlidingWindow {
    limit: u16,
    window: Duration,
    previous_sampler: InMemorySampler,
    current_sampler: InMemorySampler,
}

trait Sampler {
    fn new(window: Duration) -> Self;
    fn increment(&mut self, limit: u16) -> Option<()>;
    fn get_count(&self) -> u16;
    fn get_created_at(&self) -> Instant;
    fn get_approx(&self, next_window_duration: Duration) -> u64;
}

#[derive(Debug, Copy, Clone)]
struct InMemorySampler {
    window: Duration,
    count: u16,
    created_at: Instant,
}

impl RateLimiter {
    pub fn new(limit: u16, window: Duration) -> Self {
        RateLimiter {
            limit,
            window,
            state: HashMap::new(),
        }
    }

    pub fn can_resume(&mut self, ip: IpAddr) -> bool {
        let mut result = false;
        self.state
            .entry(ip)
            .and_modify(|x| result = x.can_resume())
            .or_insert_with(|| {
                let mut new_ip_state = SlidingWindow::new(self.limit, self.window);
                result = new_ip_state.can_resume();
                new_ip_state
            });
        result
    }

    pub fn garbage_collect(&mut self) {
        // inspired by https://blog.nginx.org/blog/rate-limiting-nginx
        //
        // "Additionally, to prevent memory from being exhausted, every time NGINX creates a new
        // entry it removes up to two entries that have not been used in the previous 60
        // seconds."
        const ITEMS: usize = 2;

        let garbage: heapless::Vec<IpAddr, ITEMS> = self
            .state
            .iter()
            .filter(|(_, v)| v.get_last_sample_created_at().elapsed() > 2 * self.window)
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
    pub fn new(limit: u16, window: Duration) -> Self {
        let mut sanitized_limit = limit;
        if limit == u16::MAX {
            sanitized_limit = limit - 1;
        }

        SlidingWindow {
            limit: sanitized_limit,
            window,
            previous_sampler: InMemorySampler::new(window),
            current_sampler: InMemorySampler::new(window),
        }
    }

    pub fn can_resume(&mut self) -> bool {
        if self.limit == 0 {
            return false;
        }

        if self.current_sampler.increment(self.limit).is_none() {
            self.shuffle_samples();
            self.current_sampler.increment(self.limit);
        }

        let elapsed = self.current_sampler.get_created_at() + self.current_sampler.get_created_at().elapsed()
            - (self.previous_sampler.get_created_at() + self.window);
        let approx = self.previous_sampler.get_approx(elapsed);
        let current_count = self.current_sampler.get_count();
        u64::from(self.limit) >= approx + u64::from(current_count)
    }

    pub fn get_last_sample_created_at(&self) -> Instant {
        self.current_sampler.created_at
    }

    fn shuffle_samples(&mut self) {
        self.previous_sampler = self.current_sampler;
        self.current_sampler = InMemorySampler::new(self.window);
    }
}

impl InMemorySampler {
    fn is_expired(&self) -> bool {
        self.created_at.elapsed().as_millis() > self.window.as_millis()
    }
}

impl Sampler for InMemorySampler {
    fn new(window: Duration) -> Self {
        InMemorySampler {
            window,
            count: 0,
            created_at: Instant::now(),
        }
    }

    fn increment(&mut self, limit: u16) -> Option<()> {
        if self.is_expired() {
            return None;
        }

        if limit >= self.count {
            self.count += 1;
        }
        Some(())
    }

    fn get_count(&self) -> u16 {
        self.count
    }

    fn get_created_at(&self) -> Instant {
        self.created_at
    }

    fn get_approx(&self, next_window_duration: Duration) -> u64 {
        if self.window > next_window_duration {
            return u64::from(self.count) * (self.window.as_secs() - next_window_duration.as_secs())
                / self.window.as_secs();
        }

        0
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
        let mut r = RateLimiter::new(50, Duration::new(60, 0));
        let ip = Ipv4Addr::new(1, 1, 1, 1).into();
        for _ in 0..42 {
            assert!(r.can_resume(ip), "should resume until limit is not reached")
        }

        sleep(Duration::from_secs(60 + 15)).await;
        for _ in 0..19 {
            assert!(r.can_resume(ip), "should resume for 42 * ((60-15)/60) + 19 = 50");
        }

        assert!(!r.can_resume(ip), "should break for 42 * ((60-15)/60) + 20 = 51");

        sleep(Duration::from_secs(3)).await;
        assert!(r.can_resume(ip), "should resume for 42 * ((60-(15+3))/60) + 21 = 50");
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_gc() {
        let mut limiter = RateLimiter::new(10, Duration::new(60, 0));
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

        sleep(Duration::from_secs(61)).await;
        assert!(limiter.can_resume(ips[0]));
        limiter.garbage_collect();
        assert_eq!(ips.len(), limiter.len());

        sleep(Duration::from_secs(60)).await;
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
    async fn test_inmemory_is_expired() {
        let mut r = InMemorySampler::new(Duration::new(60, 0));
        let limit = 50;
        assert!(r.increment(limit).is_some(), "should return Some when not expired");

        sleep(Duration::from_secs(60)).await;
        assert!(r.increment(limit).is_some(), "should return Some when still not expired");

        sleep(Duration::from_secs(1)).await;
        assert!(r.increment(limit).is_none(), "should return None when expired");
    }

    #[tokio::test]
    async fn test_inmemory_get_count() {
        let mut r = InMemorySampler::new(Duration::new(1, 0));
        let limit = 1;
        assert_eq!(0, r.get_count());
        r.increment(limit).unwrap();
        assert_eq!(1, r.get_count());

        for _ in 1..5 {
            r.increment(limit).unwrap();
            assert_eq!(2, r.get_count(), "counter should not increase after crossing the limit");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_inmemory_get_approx() {
        let mut r = InMemorySampler::new(Duration::new(60, 0));
        sleep(Duration::from_secs(60)).await;
        for _ in 0..42 {
            r.increment(50);
        }

        let start = Instant::now();
        assert_eq!(42, r.get_count());
        assert_eq!(42, r.get_approx(start.elapsed()));
        sleep(Duration::from_secs(15)).await;
        assert_eq!(31, r.get_approx(start.elapsed()));
    }
}
