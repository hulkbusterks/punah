//! Operations carried by protocol frames.

/// Stable wire operation identifiers inherited from the Redgo protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum Opcode {
    Get = 0x01,
    Set = 0x02,
    Delete = 0x03,
    Ping = 0x04,
    Pong = 0x05,
    Error = 0xff,
}

impl Opcode {
    pub(crate) const fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            0x01 => Self::Get,
            0x02 => Self::Set,
            0x03 => Self::Delete,
            0x04 => Self::Ping,
            0x05 => Self::Pong,
            0xff => Self::Error,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Opcode;

    #[test]
    fn known_values_round_trip() {
        for opcode in [
            Opcode::Get,
            Opcode::Set,
            Opcode::Delete,
            Opcode::Ping,
            Opcode::Pong,
            Opcode::Error,
        ] {
            assert_eq!(Opcode::from_byte(opcode as u8), Some(opcode));
        }
        assert_eq!(Opcode::from_byte(0), None);
    }
}
