/*----------------------------------------------------------------------------------------------------------
 *  Copyright (c) Peter Bjorklund. All rights reserved. https://github.com/conclave-rust/room-session
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------------------*/
//! The Conclave Net Layer
//!
//! Easier to handle incoming network commands and construct outgoing messages

use std::io;
use std::time::Instant;

use flood_rs::{WriteOctetStream, ReadOctetStream};

use conclave_room_serialize::{ClientReceiveCommand, RoomInfoCommand, ServerReceiveCommand};
use conclave_room_session::{ConnectionIndex, Room};

pub struct NetworkConnection {
    pub id: ConnectionIndex,
    pub room: Room,
}

pub trait SendDatagram {
    fn send(&self, stream: &mut dyn WriteOctetStream) -> io::Result<()>;
}

impl SendDatagram for Room {
    fn send(&self, stream: &mut dyn WriteOctetStream) -> io::Result<()> {
        let room_info_command = RoomInfoCommand {
            term: self.term,
            leader_index: if let Some(index) = self.leader_index {
                index.0
            } else {
                0xff
            } as u8,
            client_infos: vec![],
        };
        let client_receive_command = ClientReceiveCommand::RoomInfoType(room_info_command);
        client_receive_command.to_octets(stream)?;
        Ok(())
    }
}

pub trait ReceiveDatagram {
    fn receive(
        &mut self,
        connection_id: ConnectionIndex,
        now: Instant,
        stream: &mut dyn ReadOctetStream,
    ) -> io::Result<()> where Self: Sized;
}

impl ReceiveDatagram for Room {
    fn receive(
        &mut self,
        connection_id: ConnectionIndex,
        now: Instant,
        in_stream: &mut dyn ReadOctetStream,
    ) -> io::Result<()> {
        if !self.connections.contains_key(&connection_id) {
            return Err(io::Error::new(io::ErrorKind::Other, format!("there is no connection {}", connection_id)));
        }

        let command = ServerReceiveCommand::from_stream(in_stream)?;
        match command {
            ServerReceiveCommand::PingCommandType(ping_command) => {
                self.on_ping(
                    connection_id,
                    ping_command.term,
                    &ping_command.has_connection_to_leader,
                    ping_command.knowledge,
                    now,
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::time::Instant;

    use conclave_room_serialize::PING_COMMAND_TYPE_ID;
    use conclave_room_session::Room;
    use flood_rs::{InOctetStream, OutOctetStream};

    use crate::{ReceiveDatagram, SendDatagram};

    #[test]
    fn check_send() {
        let room = Room::new();
        let mut out_stream = OutOctetStream::default();
        room.send(&mut out_stream).unwrap();

        assert_eq!(vec![0x2a, 0x00, 0x00, 0x00, 0xff], out_stream.data);
    }

    #[test]
    fn on_ping() {
        const EXPECTED_KNOWLEDGE_VALUE: u64 = 17718865395771014920;
        let octets = [
            PING_COMMAND_TYPE_ID,
            0x00, // Term
            0x20,
            0xF5, // Knowledge
            0xE6,
            0x0E,
            0x32,
            0xE9,
            0xE4,
            0x7F,
            0x08,
            0x01, // Has connection to leader
        ];
        let receive_cursor = Cursor::new(octets.to_vec());
        let mut in_stream = InOctetStream::new_from_cursor(receive_cursor);

        let mut room = Room::new();
        let now = Instant::now();
        let first_connection_id = room.create_connection(now);
        let receive_result = room.receive(first_connection_id, now, &mut in_stream);
        assert!(receive_result.is_ok());

        let connection_after_receive = room.connections.get(&first_connection_id).unwrap();
        assert_eq!(connection_after_receive.knowledge.0, EXPECTED_KNOWLEDGE_VALUE);
    }
}
