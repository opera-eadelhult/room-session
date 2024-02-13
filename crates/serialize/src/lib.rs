/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/piot/conclave-room-serialize-rs
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
//! The Conclave Room Protocol Serialization

use std::io::{Error, ErrorKind, Result};

use conclave_types::{Term, Knowledge};
use flood_rs::{ReadOctetStream, WriteOctetStream};

use crate::ClientReceiveCommand::RoomInfoType;
use crate::ServerReceiveCommand::PingCommandType;

/// Sent from Client to Server
#[derive(Debug, PartialEq)]

pub struct PingCommand {
    pub term: Term,
    pub knowledge: Knowledge,
    pub has_connection_to_leader: bool,
}

impl PingCommand {
    pub fn to_octets(&self, stream: &mut dyn WriteOctetStream) -> Result<()> {
        stream.write_u16(self.term.0)?;
        stream.write_u64(self.knowledge.0)?;
        stream.write_u8(if self.has_connection_to_leader {
            0x01
        } else {
            0x00
        })?;

        Ok(())
    }

    pub fn from_cursor(stream: &mut dyn ReadOctetStream) -> Result<Self> {
        Ok(Self {
            term: Term(stream.read_u16()?),
            knowledge: Knowledge(stream.read_u64()?),
            has_connection_to_leader: stream.read_u8()? != 0,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ClientInfo {
    pub custom_user_id: u64,
    pub connection_index: u8,
}

/// Sent from Server to Client
#[derive(Debug, PartialEq)]
pub struct RoomInfoCommand {
    pub term: Term,
    pub leader_index: u8,
    pub client_infos: Vec<ClientInfo>,
}

impl RoomInfoCommand {
    pub fn to_octets(&self, stream: &mut dyn WriteOctetStream) -> Result<()> {
        stream.write_u16(self.term.0)?;
        stream.write_u8(self.client_infos.len() as u8)?;
        for client_info in self.client_infos.iter() {
            stream.write_u8(client_info.connection_index)?;
            stream.write_u64(client_info.custom_user_id)?;
        }
        stream.write_u8(self.leader_index)?;

        Ok(())
    }

    pub fn from_cursor(stream: &mut dyn ReadOctetStream) -> Result<Self> {
        let term = Term(stream.read_u16()?);
        let length = stream.read_u8()? as usize;
        let slice = &mut vec![ClientInfo {
            custom_user_id: 0,
            connection_index: 0,
        }][..length];
        for client_info in slice.iter_mut().take(length) {
            *client_info = ClientInfo {
                connection_index: stream.read_u8()?,
                custom_user_id: stream.read_u64()?,
            }
        }
        Ok(Self {
            term,
            leader_index: stream.read_u8()?,
            client_infos: slice.to_vec(),
        })
    }
}

#[derive(Debug)]
pub enum ServerReceiveCommand {
    PingCommandType(PingCommand),
}

impl ServerReceiveCommand {
    pub fn to_octets(&self, stream: &mut impl WriteOctetStream) -> Result<()> {
        let command_type_id = match self {
            PingCommandType(_) => PING_COMMAND_TYPE_ID,
            // _ => return Err(format!("unsupported command {:?}", self)),
        };

        stream.write_u8(command_type_id)?;

        match self {
            PingCommandType(ping_command) => {
                ping_command.to_octets(stream)?;
            } // _ => return Err(format!("unknown command enum {:?}", self)),
        }

        Ok(())
    }

    pub fn from_stream(stream: &mut dyn ReadOctetStream) -> Result<ServerReceiveCommand> {
        let command_type_id = stream.read_u8()?;
        match command_type_id {
            PING_COMMAND_TYPE_ID => Ok(PingCommandType(PingCommand::from_cursor(stream)?)),
            _ => Err(Error::new(
                ErrorKind::Other,
                format!("unknown command 0x{:x}", command_type_id),
            )),
        }
    }
}

pub const PING_COMMAND_TYPE_ID: u8 = 0x01;
pub const ROOM_INFO_COMMAND_TYPE_ID: u8 = 0x02;

#[derive(Debug)]
pub enum ClientReceiveCommand {
    RoomInfoType(RoomInfoCommand),
}

impl ClientReceiveCommand {
    pub fn to_octets<T: WriteOctetStream>(&self, stream: &mut T) -> Result<()> {
        let command_type_id = match self {
            RoomInfoType(_) => ROOM_INFO_COMMAND_TYPE_ID,
            // _ => return Err(format!("unsupported command {:?}", self)),
        };

        stream.write_u8(command_type_id)?;

        match self {
            RoomInfoType(room_info_command) => room_info_command.to_octets(stream)?, // _ => return Err(format!("unknown command enum {:?}", self)),
        }

        Ok(())
    }

    pub fn from_octets(stream: &mut impl ReadOctetStream) -> Result<ClientReceiveCommand> {
        let command_type_id = stream.read_u8()?;
        match command_type_id {
            ROOM_INFO_COMMAND_TYPE_ID => Ok(RoomInfoType(RoomInfoCommand::from_cursor(stream)?)),
            _ => Err(Error::new(
                ErrorKind::Other,
                format!("unknown command 0x{:x}", command_type_id),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use conclave_types::{Term, Knowledge};
    use flood_rs::{InOctetStream, OutOctetStream};

    use crate::ClientReceiveCommand::RoomInfoType;
    use crate::ServerReceiveCommand::PingCommandType;
    use crate::{
        ClientReceiveCommand, PingCommand, ServerReceiveCommand, PING_COMMAND_TYPE_ID,
        ROOM_INFO_COMMAND_TYPE_ID,
    };

    #[test]
    fn check_serializer() {
        let ping_command = PingCommand {
            term: Term(32),
            knowledge: Knowledge(444441),
            has_connection_to_leader: false,
        };

        let mut out_stream = OutOctetStream::new();
        ping_command.to_octets(&mut out_stream).unwrap();

        let mut in_stream = InOctetStream::new(out_stream.data);
        let in_stream_ref = &mut in_stream;
        let deserialized_ping_command = PingCommand::from_cursor(in_stream_ref).unwrap();

        println!("before {:?}", &ping_command);
        println!("after {:?}", &deserialized_ping_command);
        assert_eq!(ping_command, deserialized_ping_command);
    }

    #[test]
    fn check_server_receive_message() {
        const EXPECTED_KNOWLEDGE_VALUE: u64 = 17718865395771014920;

        let octets = [
            PING_COMMAND_TYPE_ID,
            0x00,
            0x20, // Term
            0xF5,
            0xE6,
            0x0E,
            0x32,
            0xE9,
            0xE4,
            0x7F,
            0x08, // Knowledge
            0x01, // Has Connection
        ];

        let mut in_stream = InOctetStream::new(Vec::from(octets));

        let message = ServerReceiveCommand::from_stream(&mut in_stream).unwrap();

        match message {
            PingCommandType(ping_command) => {
                println!("received {:?}", &ping_command);
                assert_eq!(ping_command.term.0, 0x20);
                assert_eq!(ping_command.knowledge.0, EXPECTED_KNOWLEDGE_VALUE);
                assert_eq!(ping_command.has_connection_to_leader, true);
            } // _ => assert!(false, "should be ping command"),
        }
    }

    #[test]
    fn check_client_receive_message() {
        const EXPECTED_LEADER_INDEX: u8 = 1;

        let octets = [
            ROOM_INFO_COMMAND_TYPE_ID,
            0x00,                  // Term
            0x4A,                  // Term (lower)
            0x00,                  // Number of client infos that follows
            EXPECTED_LEADER_INDEX, // Leader index
        ];

        let mut in_stream = InOctetStream::new(Vec::from(octets));

        let message = ClientReceiveCommand::from_octets(&mut in_stream).unwrap();

        match message {
            RoomInfoType(room_info) => {
                println!("received {:?}", &room_info);
                assert_eq!(room_info.term.0, 0x4A);
                assert_eq!(room_info.leader_index, EXPECTED_LEADER_INDEX);
            } // _ => assert!(false, "should be room info command"),
        }
    }
}
