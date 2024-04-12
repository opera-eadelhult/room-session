use std::time::Instant;
use log::trace;

use crate::metrics::RateMetrics;

/// Resulting Assessment made by [ConnectionQuality]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum QualityAssessment {
    NeedMoreInformation,
    RecommendDisconnect,
    Acceptable,
    Good,
}

/// Evaluate room connection quality
#[derive(Debug)]
pub struct ConnectionQuality {
    pub last_ping_at: Instant,
    pub pings_per_second: RateMetrics,
    pub assessment: QualityAssessment,
    threshold: f32,
}

impl ConnectionQuality {
    pub fn new(threshold: f32, time: Instant) -> Self {
        Self {
            assessment: QualityAssessment::NeedMoreInformation,
            last_ping_at: Instant::now(),
            pings_per_second: RateMetrics::new(time),
            threshold,
        }
    }

    pub fn on_ping(&mut self, time: Instant) {
        self.last_ping_at = time;
        self.pings_per_second.increment();
    }

    pub fn update(&mut self, time: Instant) {
        if !self.pings_per_second.has_enough_time_passed(time) {
            self.assessment = QualityAssessment::NeedMoreInformation;
        } else {
            let pings_per_second = self.pings_per_second.calculate_rate(time);
            self.assessment = if pings_per_second < self.threshold {
                QualityAssessment::RecommendDisconnect
            } else if pings_per_second > self.threshold * 2.0 {
                QualityAssessment::Good
            } else {
                QualityAssessment::Acceptable
            };
            trace!("pings_per_second {}, assessment {:?}", pings_per_second, self.assessment);
        }
    }
}
