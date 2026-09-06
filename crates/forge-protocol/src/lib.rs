use std::io::{self, Read, Write};

pub const MAGIC: [u8; 4] = *b"FRGE";
pub const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Submit = 1,
    TaskRequest = 2,
    TaskResult = 3,
    Heartbeat = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: MessageKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidMagic,
    UnsupportedVersion(u8),
    UnknownMessage(u8),
    FrameTooLarge(u32),
}

impl MessageKind {
    fn from_byte(value: u8) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::Submit),
            2 => Ok(Self::TaskRequest),
            3 => Ok(Self::TaskResult),
            4 => Ok(Self::Heartbeat),
            other => Err(ProtocolError::UnknownMessage(other)),
        }
    }
}

impl Frame {
    pub fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&MAGIC)?;
        writer.write_all(&[VERSION, self.kind as u8])?;
        writer.write_all(&(self.payload.len() as u32).to_be_bytes())?;
        writer.write_all(&self.payload)
    }

    pub fn decode<R: Read>(reader: &mut R) -> Result<Self, Box<dyn std::error::Error>> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(Box::new(ProtocolError::InvalidMagic));
        }

        let mut header = [0u8; 6];
        reader.read_exact(&mut header)?;
        if header[0] != VERSION {
            return Err(Box::new(ProtocolError::UnsupportedVersion(header[0])));
        }

        let length = u32::from_be_bytes([header[2], header[3], header[4], header[5]]);
        if length > 16 * 1024 * 1024 {
            return Err(Box::new(ProtocolError::FrameTooLarge(length)));
        }

        let kind = MessageKind::from_byte(header[1]).map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?;
        let mut payload = vec![0u8; length as usize];
        reader.read_exact(&mut payload)?;
        Ok(Self { kind, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let frame = Frame {
            kind: MessageKind::Heartbeat,
            payload: b"worker-1".to_vec(),
        };
        let mut bytes = Vec::new();
        frame.encode(&mut bytes).unwrap();
        let decoded = Frame::decode(&mut bytes.as_slice()).unwrap();
        assert_eq!(decoded, frame);
    }
}
