/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/conclave-rust/room-session
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
//! The Conclave Logic for a Room
//!
//! Evaluating connection quality for all connections attached to the room. Using "votes" from the connections, together with
//! [Knowledge] and [ConnectionQuality] it determines which connection should be appointed leader.

mod connection_quality;
mod metrics;

use core::fmt;
use std::collections::HashMap;
use std::time::Instant;

use crate::connection_quality::ConnectionQuality;
use conclave_types::{ConnectionToLeader, Knowledge, Term};
use connection_quality::QualityAssessment;

/// ID or index for a room connection
#[derive(Default, Debug, Clone, Copy, Eq, Hash, PartialEq, PartialOrd)]
pub struct ConnectionIndex(pub u16);

impl fmt::Display for ConnectionIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConnectionIndex: {}", self.0)
    }
}

impl ConnectionIndex {
    pub fn new(value: u16) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u16 {
        self.0
    }

    pub fn next(&mut self) {
        self.0 += 1;
    }
}

#[derive(Debug)]
pub enum ConnectionState {
    Online,
    Disconnected,
}

/// A Room Connection
#[derive(Debug)]
pub struct Connection {
    pub id: ConnectionIndex,
    quality: ConnectionQuality,
    pub knowledge: Knowledge,
    pub state: ConnectionState,
    pub last_reported_term: Term,
    pub has_connection_host: ConnectionToLeader,
}

impl Connection {
    fn new(
        connection_id: ConnectionIndex,
        term: Term,
        time: Instant,
        pings_per_second_threshold: f32,
    ) -> Self {
        Connection {
            has_connection_host: ConnectionToLeader::Unknown,
            last_reported_term: term,
            id: connection_id,
            quality: ConnectionQuality::new(pings_per_second_threshold, time),
            knowledge: Knowledge(0),
            state: ConnectionState::Online,
        }
    }

    fn on_ping(
        &mut self,
        term: Term,
        has_connection_to_host: &ConnectionToLeader,
        knowledge: Knowledge,
        time: Instant,
    ) {
        self.last_reported_term = term;
        self.has_connection_host = has_connection_to_host.clone();
        self.quality.on_ping(time);
        self.knowledge = knowledge;
    }

    fn update(&mut self, time: Instant) {
        self.quality.update(time);
    }

    pub fn assessment(&self) -> QualityAssessment {
        self.quality.assessment
    }
}

#[derive(Debug)]
pub struct RoomConfig {
    pub allowed_to_remove_single_leader: bool,
    pub pings_per_second_threshold: f32,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            allowed_to_remove_single_leader: false,
            pings_per_second_threshold: 10.0,
        }
    }
}

impl RoomConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allow_remove_single_leader(mut self) -> Self {
        self.allowed_to_remove_single_leader = true;
        self
    }

    pub fn pings_per_second_threshold(mut self, threshold: f32) -> Self {
        self.pings_per_second_threshold = threshold;
        self
    }

    pub fn build(self) -> Room {
        Room::new_with_config(self)
    }
}

/// Contains the Room [Connection]s as well the appointed Leader.
#[derive(Debug)]
pub struct Room {
    pub id: ConnectionIndex,
    pub connections: HashMap<ConnectionIndex, Connection>,
    pub leader_index: Option<ConnectionIndex>,
    pub term: Term,
    pub config: RoomConfig,
}

impl Default for Room {
    fn default() -> Self {
        Self {
            id: ConnectionIndex(0),
            connections: HashMap::new(),
            leader_index: None,
            term: Term(0),
            config: Default::default(),
        }
    }
}

impl Room {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn new_with_config(config: RoomConfig) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    /// checks if most connections, that are on the same term, has lost connection to leader
    fn has_most_lost_connection_to_leader(&self) -> bool {
        let mut disappointed_count = 0;
        for (_, connection) in self.connections.iter() {
            if connection.has_connection_host == ConnectionToLeader::Disconnected
                && connection.last_reported_term == self.term
            {
                disappointed_count += 1;
            }
        }
        disappointed_count >= self.connections.len()
    }

    fn connection_with_most_knowledge_and_acceptable_quality(
        &self,
        exclude_index: Option<ConnectionIndex>,
    ) -> Option<ConnectionIndex> {
        let mut knowledge: Knowledge = Knowledge(0);
        let mut connection_index: Option<ConnectionIndex> = None;

        for (_, connection) in self.connections.iter() {
            if (connection.knowledge >= knowledge || connection_index.is_none())
                && exclude_index.is_none()
                || connection.id != exclude_index.unwrap()
            {
                knowledge = connection.knowledge;
                connection_index = Some(connection.id);
            }
        }

        connection_index
    }

    fn switch_leader_to_best_knowledge_and_quality(&mut self) {
        self.leader_index =
            self.connection_with_most_knowledge_and_acceptable_quality(self.leader_index);
        // We start a new term, since we have a new leader
        self.term.next();
    }

    fn change_leader_if_down_voted(&mut self) -> bool {
        if self.leader_index.is_none() {
            return false;
        }

        if self.has_most_lost_connection_to_leader() {
            self.switch_leader_to_best_knowledge_and_quality();
            return true;
        }

        false
    }

    fn is_possble_to_switch_leader(&self) -> bool {
        self.connections.len() > 1 || self.config.allowed_to_remove_single_leader
    }

    fn switch_leader_if_non_responsive(&mut self) {
        if self.leader_index.is_none() {
            return;
        }

        let leader_connection = self.connections.get(&self.leader_index.unwrap()).unwrap();

        if leader_connection.assessment() == QualityAssessment::RecommendDisconnect
            && self.is_possble_to_switch_leader()
        {
            self.switch_leader_to_best_knowledge_and_quality()
        }
    }

    fn find_unique_connection_index(&self) -> ConnectionIndex {
        let mut candidate = self.id;

        while self.connections.contains_key(&candidate) {
            candidate.next();
            if candidate == self.id {
                panic!("No unique connection index available");
            }
        }

        candidate
    }

    pub fn create_connection(&mut self, time: Instant) -> ConnectionIndex {
        self.id.next();
        let connection_id = self.find_unique_connection_index();
        let connection = Connection::new(
            connection_id,
            self.term,
            time,
            self.config.pings_per_second_threshold,
        );
        self.connections.insert(self.id, connection);

        if self.leader_index.is_none() {
            self.leader_index = Some(self.id);
        }

        self.id
    }

    pub fn update(&mut self, time: Instant) {
        for connection in self.connections.values_mut() {
            connection.update(time);
        }
        let leader_was_changed = self.change_leader_if_down_voted();
        if leader_was_changed {
            return;
        }

        self.switch_leader_if_non_responsive();
    }

    /// Receiving a ping command from a connection
    pub fn on_ping(
        &mut self,
        connection_index: ConnectionIndex,
        term: Term,
        has_connection_to_host: &ConnectionToLeader,
        knowledge: Knowledge,
        time: Instant,
    ) {
        let connection = self.connections.get_mut(&connection_index).unwrap();
        connection.on_ping(term, &has_connection_to_host, knowledge, time)
    }

    pub fn get_mut(&mut self, connection_index: ConnectionIndex) -> &mut Connection {
        self.connections.get_mut(&connection_index).unwrap()
    }

    pub fn get(&self, connection_index: ConnectionIndex) -> &Connection {
        self.connections.get(&connection_index).unwrap()
    }

    pub fn destroy_connection(&mut self, connection_index: ConnectionIndex) {
        if let Some(leader_index) = self.leader_index {
            if leader_index == connection_index {
                // If it was the leader, we must select a new leader
                self.switch_leader_to_best_knowledge_and_quality();
            }
        }
        self.connections.remove(&connection_index);
    }
}

#[cfg(test)]
mod tests {
    use core::time;
    use std::time::{Duration, Instant};

    use crate::{QualityAssessment, Room, RoomConfig};
    use conclave_types::{ConnectionToLeader, Knowledge, Term};

    #[test]
    fn check_ping() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection_id = room.create_connection(now);
        assert_eq!(connection_id.value(), 1);
        let knowledge: Knowledge = Knowledge(42);
        let term: Term = Term(1);

        {
            room.on_ping(
                connection_id,
                term,
                &ConnectionToLeader::Connected,
                knowledge,
                now,
            );

            let time_in_future = now + Duration::new(10, 0);
            room.on_ping(
                connection_id,
                term,
                &ConnectionToLeader::Connected,
                knowledge,
                time_in_future,
            );
            room.update(time_in_future);
            assert_eq!(
                room.get(connection_id).quality.assessment,
                QualityAssessment::RecommendDisconnect
            );
        }
    }

    #[test]
    fn remove_connection() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection_id = room.create_connection(now);
        assert_eq!(room.connections.len(), 1);
        assert_eq!(connection_id.value(), 1);

        room.destroy_connection(connection_id);
        assert_eq!(room.connections.len(), 0);
    }

    #[test]
    fn change_leader() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection_id = room.create_connection(now);
        let term = room.term;
        assert_eq!(connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let supporter_connection_id = room.create_connection(now);

        assert_eq!(supporter_connection_id.value(), 2);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let time_in_future = now + Duration::new(10, 0);

        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);

        room.on_ping(
            supporter_connection_id,
            term,
            &has_connection_to_host,
            knowledge,
            time_in_future,
        );

        room.update(time_in_future);

        // Only the supporter connection has reported, so the leader_connection should be disconnected
        assert_eq!(room.leader_index.unwrap().value(), 2);
    }

    #[test]
    fn retain_leader_if_single_leader_times_out() {
        let mut room = Room::new();
        let now = Instant::now();
        let single_leader_connection_id = room.create_connection(now);
        let term = room.term;
        assert_eq!(single_leader_connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let time_in_future = now + Duration::new(40, 0);

        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);

        room.on_ping(
            single_leader_connection_id,
            term,
            &has_connection_to_host,
            knowledge,
            time_in_future,
        );

        room.update(time_in_future);

        // the single leader has timed out, but should be retained by default
        assert_eq!(room.leader_index.unwrap().value(), 1);
    }

    #[test]
    fn custom_timeout_config() {
        let mut room = RoomConfig::new()
            .allow_remove_single_leader()
            .pings_per_second_threshold(40.0)
            .build();
        let now = Instant::now();
        let single_leader_connection_id = room.create_connection(now);
        let term = room.term;
        assert_eq!(single_leader_connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let time_in_future = now + Duration::new(39, 0);

        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);

        room.on_ping(
            single_leader_connection_id,
            term,
            &has_connection_to_host,
            knowledge,
            time_in_future,
        );

        assert_eq!(room.leader_index.unwrap().value(), 1);

        room.update(time_in_future);

        let should_time_out_time = now + Duration::new(41, 0);
        room.on_ping(
            single_leader_connection_id,
            term,
            &has_connection_to_host,
            knowledge,
            should_time_out_time,
        );

        // the single leader should have timed out now
        assert!(room.leader_index.is_none());
    }

    #[test]
    fn kick_leader_if_single_leader_times_out() {
        let mut room = RoomConfig::new().allow_remove_single_leader().build();
        let now = Instant::now();
        let single_leader_connection_id = room.create_connection(now);
        let term = room.term;
        assert_eq!(single_leader_connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let time_in_future = now + Duration::new(40, 0);

        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);

        room.on_ping(
            single_leader_connection_id,
            term,
            &has_connection_to_host,
            knowledge,
            time_in_future,
        );

        room.update(time_in_future);

        // the single leader has timed out, and is removed
        assert!(room.leader_index.is_none());
    }

    #[test]
    fn change_leader_when_destroying_leader_connection() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection_id = room.create_connection(now);
        assert_eq!(room.term.value(), 0);
        assert_eq!(connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);
        room.destroy_connection(connection_id);
        assert_eq!(room.term.value(), 1);
        assert!(room.leader_index.is_none())
    }
}
