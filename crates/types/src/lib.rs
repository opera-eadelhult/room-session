/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/conclave-rust/room-session
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
use core::fmt;

pub type GuiseUserSessionId = u64;
pub type ClientNonce = u64;
pub type SessionId = u64;

/// The term that Leader is currently running. The term is increased whenever a leader is appointed.
#[derive(Default, Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Term(pub u16);

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[term {}]", self.0)
    }
}

impl Term {
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

/// The knowledge of the game state, typically the tick ID.
#[derive(Default, Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct Knowledge(pub u64);

impl fmt::Display for Knowledge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Knowledge: {}", self.0)
    }
}

impl Knowledge {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ConnectionToLeader {
    Unknown,
    Connected,
    Disconnected,
}

impl ConnectionToLeader {
    pub fn to_u8(&self) -> u8 {
        match self {
            ConnectionToLeader::Unknown => 0,
            ConnectionToLeader::Connected => 1,
            ConnectionToLeader::Disconnected => 2,
        }
    }

    pub fn from_u8(value: u8) -> Option<ConnectionToLeader> {
        match value {
            0 => Some(ConnectionToLeader::Unknown),
            1 => Some(ConnectionToLeader::Connected),
            2 => Some(ConnectionToLeader::Disconnected),
            _ => None,
        }
    }
}

#[cfg(test)]
mod term_tests {
    use crate::Term;

    #[test]
    fn next_term() {
        let mut term = Term::new(10);
        term.next();
        assert_eq!(term.value(), 11);
    }
}

#[cfg(test)]
mod knowledge_tests {
    use crate::Knowledge;

    #[test]
    fn new_knowledge() {
        let knowledge = Knowledge::new(100);
        assert_eq!(knowledge.value(), 100);
    }
}
