/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/conclave-rust/room-session
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
//! The Conclave Logic for a Room
//!
//! Evaluating connection quality for all connections attached to the room. Using "votes" from the connections, together with
//! [Knowledge] and [ConnectionQuality] it determines which connection should be appointed leader.

extern crate core;

use core::fmt;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use log::{debug, info, trace};

use conclave_types::{ConnectionToLeader, Knowledge, Term};
use connection_quality::QualityAssessment;
use serde::{Deserialize, Serialize};

use crate::connection_quality::ConnectionQuality;

mod connection_quality;
mod metrics;

/// ID or index for a room connection
#[derive(Default, Debug, Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ConnectionIndex(pub u16);

impl fmt::Display for ConnectionIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[conn_id: {}]", self.0)
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
    pub last_reported_term: Option<Term>,
    pub has_connection_host: ConnectionToLeader,
    pub debug_name: Option<String>,
}

impl fmt::Display for Connection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[conn id:{} (name:{:?})  knowledge:{}, connectedToHost:{:?}, knownTerm:{:?}, quality:{}]", self.id, self.debug_name, self.knowledge, self.has_connection_host, self.last_reported_term, self.quality)
    }
}

impl Connection {
    fn new(
        connection_id: ConnectionIndex,
        time: Instant,
        pings_per_second_threshold: f32,
    ) -> Self {
        Connection {
            has_connection_host: ConnectionToLeader::Unknown,
            last_reported_term: None,
            id: connection_id,
            quality: ConnectionQuality::new(pings_per_second_threshold, time),
            knowledge: Knowledge(0),
            state: ConnectionState::Online,
            debug_name: None,
        }
    }

    fn on_ping(
        &mut self,
        term: Term,
        has_connection_to_host: &ConnectionToLeader,
        knowledge: Knowledge,
        time: Instant,
    ) {
        self.last_reported_term = Some(term);
        self.has_connection_host = *has_connection_to_host;
        self.quality.on_ping(time);
        self.knowledge = knowledge;
    }

    fn update(&mut self, time: Instant) {
        self.quality.update(time);
        trace!("update {}", self);
    }

    pub fn assessment(&self) -> QualityAssessment {
        self.quality.assessment
    }
}

/// Configuration for a Room
#[derive(Debug)]
pub struct RoomConfig {
    pub allowed_to_remove_single_leader: bool,
    pub pings_per_second_threshold: f32,
    pub disconnect_bad_connections: bool,
    pub destroy_disconnected_connections: bool,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            allowed_to_remove_single_leader: false,
            pings_per_second_threshold: 5.0,
            disconnect_bad_connections: true,
            destroy_disconnected_connections: false,
        }
    }
}

/// Room config builder
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

    pub fn with_disconnect_bad_connections(mut self, should_disconnect: bool) -> Self {
        self.disconnect_bad_connections = should_disconnect;
        self
    }

    pub fn with_destroy_disconnected_connections(mut self, should_destroy: bool) -> Self {
        self.destroy_disconnected_connections = should_destroy;
        self
    }

    pub fn recommended_for_debug() -> Self {
        Self::default().pings_per_second_threshold(4.0)
    }

    pub fn recommended_for_release() -> Self {
        Self::default().pings_per_second_threshold(10.0)
    }

    pub fn build(self) -> Room {
        Room::new_with_config(self)
    }
}

const ABANDONED_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Contains the Room [Connection]s as well the appointed Leader.
#[derive(Debug)]
pub struct Room {
    pub id: ConnectionIndex,
    pub connections: HashMap<ConnectionIndex, Connection>,
    pub leader_index: Option<ConnectionIndex>,
    pub term: Term,
    pub config: RoomConfig,
    pub latest_ping_timestamp: Option<Instant>,
}


impl Default for Room {
    fn default() -> Self {
        Self {
            id: ConnectionIndex(0),
            connections: HashMap::new(),
            leader_index: None,
            term: Term(0),
            config: Default::default(),
            latest_ping_timestamp: None,
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
        self.connections
            .iter()
            .filter(|(_, connection)| {
                connection.has_connection_host == ConnectionToLeader::Disconnected
                    && connection.last_reported_term == Some(self.term)
            })
            .count()
            > self.connections.len() / 2
    }

    fn connection_with_most_knowledge_and_acceptable_quality(
        &self,
        exclude_index: Option<ConnectionIndex>,
    ) -> Option<ConnectionIndex> {
        self.connections
            .iter()
            .filter(|(_, connection)| exclude_index.map_or(true, |ex_id| connection.id != ex_id))
            .max_by_key(|(_, connection)| connection.knowledge)
            .map(|(_, connection)| connection.id)
    }

    fn switch_leader(&mut self, leader_index: Option<ConnectionIndex>) {
        self.leader_index = leader_index;
        // We start a new term, since we have a new leader
        self.term.next();
        debug!("elected a new leader {:?} for the term {}", self.leader_index, self.term)
    }

    fn switch_leader_to_best_knowledge_and_quality(&mut self) {
        let leader_index =
            self.connection_with_most_knowledge_and_acceptable_quality(self.leader_index);
        self.switch_leader(leader_index)
    }

    fn change_leader_if_down_voted(&mut self) -> bool {
        if self.leader_index.is_none() {
            return false;
        }

        if self.has_most_lost_connection_to_leader() {
            info!("most members have down-voted leader {}, so switching to a new one", self.leader_index.unwrap());
            self.switch_leader_to_best_knowledge_and_quality();
            return true;
        }

        false
    }

    fn is_possible_to_switch_leader(&self) -> bool {
        self.connections.len() > 1 || self.config.allowed_to_remove_single_leader
    }

    fn switch_leader_if_non_responsive(&mut self) {
        if self.leader_index.is_none() {
            return;
        }

        let leader_connection = self.connections.get(&self.leader_index.unwrap()).unwrap();
        if leader_connection.assessment() == QualityAssessment::RecommendDisconnect
            && self.is_possible_to_switch_leader()
        {
            debug!("leader {} connection has bad quality, switching to a new leader", self.leader_index.unwrap());
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
            time,
            self.config.pings_per_second_threshold,
        );

        info!("create connection {}", connection);

        if self.leader_index.is_none() {
            info!("this was first connection {}, so this will be leader:{}", &connection, self.id);
            self.switch_leader(Some(self.id));
        }

        self.connections.insert(self.id, connection);

        self.id
    }

    /// Determines if a given connection is aware of the current term.
    ///
    /// This method checks whether the connection identified by `connection_index`
    /// has acknowledged that they received information about the current term.
    ///
    /// # Arguments
    ///
    /// * `connection_index` - A unique identifier for the connection.
    ///
    /// # Returns
    ///
    /// Returns `true` if the specified connection is aware of the current term, otherwise `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// // Example usage of `connection_knows_about_current_term`.
    /// use std::time::Instant;
    /// use conclave_room_session::Room;
    /// let mut room = Room::new();
    /// let some_connection_index = room.create_connection(Instant::now());
    /// let is_aware = room.connection_knows_about_current_term(some_connection_index);
    /// if is_aware {
    ///     println!("The connection is aware of the current term.");
    /// } else {
    ///     println!("The connection is not aware of the current term.");
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// This method panics if there is no connection associated with the provided `connection_index`.
    pub fn connection_knows_about_current_term(&self, connection_index: ConnectionIndex) -> bool {
        let found_connection = self.connections.get(&connection_index).unwrap();
        if let Some(last_reported_term) = found_connection.last_reported_term {
            last_reported_term == self.term
        } else {
            false
        }
    }

    pub fn update(&mut self, time: Instant) {
        trace!("update connections {} time:{:?}", self.connections.len(), time);
        for connection in self.connections.values_mut() {
            connection.update(time);
        }

        if self.config.disconnect_bad_connections {
            let mut connection_index_vector = Vec::<ConnectionIndex>::new();
            for connection in self.connections.values_mut() {
                if connection.assessment() == QualityAssessment::RecommendDisconnect {
                    connection.state = ConnectionState::Disconnected;
                    debug!("disconnecting {}", connection);
                    if self.config.destroy_disconnected_connections {
                        connection_index_vector.push(connection.id);
                    }
                }
            }

            if self.config.destroy_disconnected_connections {
                for connection_index in connection_index_vector {
                    debug!("destroying {}", connection_index);
                    self.destroy_connection(connection_index);
                }
            }
        }

        let leader_was_changed = self.change_leader_if_down_voted();
        if leader_was_changed {
            return;
        }

        self.switch_leader_if_non_responsive();
    }

    /// True if the room has not received a ping from anyone in `ABANDONED_TIMEOUT` amount of time
    pub fn is_abandoned(&self, now: Instant) -> bool {
        let Some(prev) = self.latest_ping_timestamp else {
            // This room has never received a single ping
            return true;
        };

        now - prev > ABANDONED_TIMEOUT
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
        self.latest_ping_timestamp = Some(time);
        let connection = self.connections.get_mut(&connection_index).unwrap();
        connection.on_ping(term, has_connection_to_host, knowledge, time);
        self.update(time);
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

    pub fn set_debug_name(&mut self, connection_index: ConnectionIndex, name: &str) {
        self.connections.get_mut(&connection_index).unwrap().debug_name = Some(name.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use log::info;
    use test_log::test;

    use conclave_types::{ConnectionToLeader, Knowledge, Term};

    use crate::{QualityAssessment, Room, RoomConfig};

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
        assert_eq!(room.leader_index, Some(connection_id));

        room.destroy_connection(connection_id);
        assert_eq!(room.connections.len(), 0);
        assert_eq!(room.leader_index, None);
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

        // the single leader has timed out, but should be retained by default
        assert_eq!(room.leader_index.unwrap().value(), 1);
    }

    #[test]
    fn custom_timeout_config() {
        let mut room = RoomConfig::new()
            .allow_remove_single_leader()
            .pings_per_second_threshold(0.9)
            .build();
        let now = Instant::now();
        let single_leader_connection_id = room.create_connection(now);
        let term = room.term;
        assert_eq!(single_leader_connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let mut time = now;

        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);

        for _ in 0..2 {
            time += Duration::new(1, 0);
            room.on_ping(
                single_leader_connection_id,
                term,
                &has_connection_to_host,
                knowledge,
                time,
            );
        }

        assert_eq!(room.leader_index.unwrap().value(), 1);

        for _ in 0..2 {
            time += Duration::new(2, 0);
            room.on_ping(
                single_leader_connection_id,
                term,
                &has_connection_to_host,
                knowledge,
                time,
            );
        }

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

        // the single leader has timed out, and is removed
        assert!(room.leader_index.is_none());
    }

    #[test]
    fn change_leader_when_destroying_leader_connection() {
        let mut room = Room::new();
        let now = Instant::now();
        assert_eq!(room.term.value(), 0);
        let connection_id = room.create_connection(now);
        assert_eq!(connection_id.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);
        room.destroy_connection(connection_id);
        assert_eq!(room.term.value(), 2);
        assert!(room.leader_index.is_none())
    }

    #[test]
    fn knows_about_current_term() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection_id = room.create_connection(now);

        assert_eq!(room.connection_knows_about_current_term(connection_id), false);
        let wrong_term = Term(0);
        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);
        room.on_ping(
            connection_id,
            wrong_term,
            &has_connection_to_host,
            knowledge,
            now,
        );

        assert_eq!(room.connection_knows_about_current_term(connection_id), false);
        assert_eq!(room.term.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let time_in_future = now + Duration::new(40, 0);
        room.on_ping(
            connection_id,
            room.term,
            &has_connection_to_host,
            knowledge,
            time_in_future,
        );


        assert_eq!(room.connection_knows_about_current_term(connection_id), true);
    }

    #[test]
    fn check_set_debug_name() {
        let mut room = Room::new();
        let now = Instant::now();
        let connection_id = room.create_connection(now);
        room.set_debug_name(connection_id, "Hello");
        info!("connection: {}", room.get(connection_id))
    }

    #[test]
    fn destroy_room_with_no_ping() {
        let mut room = RoomConfig::new()
            .with_destroy_disconnected_connections(true)
            .with_disconnect_bad_connections(true)
            .build();
        let now = Instant::now();
        let connection_id = room.create_connection(now);

        assert_eq!(room.connection_knows_about_current_term(connection_id), false);
        let wrong_term = Term(0);
        let has_connection_to_host = ConnectionToLeader::Connected;
        let knowledge: Knowledge = Knowledge(42);
        room.on_ping(
            connection_id,
            wrong_term,
            &has_connection_to_host,
            knowledge,
            now,
        );

        assert_eq!(room.connection_knows_about_current_term(connection_id), false);
        assert_eq!(room.term.value(), 1);
        assert_eq!(room.leader_index.unwrap().value(), 1);

        let time_in_future = now + Duration::new(0, 500);
        assert_eq!(room.connections.len(), 1);
        room.on_ping(
            connection_id,
            room.term,
            &has_connection_to_host,
            knowledge,
            time_in_future,
        );
        assert_eq!(room.connections.len(), 1);

        assert_eq!(room.connection_knows_about_current_term(connection_id), true);

        assert_eq!(room.is_abandoned(time_in_future), false);

        let time_in_future_with_no_ping = time_in_future + Duration::new(20, 0);
        room.update(time_in_future_with_no_ping);
        assert_eq!(room.connections.len(), 0);
        assert_eq!(room.is_abandoned(time_in_future_with_no_ping), false);

        let fifteen_minutes_later = time_in_future_with_no_ping + Duration::new(15 * 60, 0);
        assert_eq!(room.is_abandoned(fifteen_minutes_later), true);
    }
}
