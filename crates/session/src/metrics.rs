/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/conclave-rust/room-session
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
use std::time::Instant;

/// Evaluating how many times something occurs every second.
#[derive(Debug)]
pub struct RateMetrics {
    count: u32,
    last_calculated_at: Instant,
}

impl RateMetrics {
    pub fn new(time: Instant) -> Self {
        Self {
            count: 0,
            last_calculated_at: time,
        }
    }

    pub fn increment(&mut self) {
        self.count += 1;
    }

    pub fn has_enough_time_passed(&self, time: Instant) -> bool {
        (time - self.last_calculated_at).as_millis() > 200
    }

    pub(crate) fn calculate_rate(&mut self, time: Instant) -> f32 {
        let elapsed_time = time - self.last_calculated_at;
        let seconds = elapsed_time.as_secs_f32();

        let rate = if seconds > 0.0 {
            self.count as f32 / seconds
        } else {
            0.0
        };

        // Reset the counter and start time for the next period
        self.count = 0;
        self.last_calculated_at = time;

        rate
    }
}
