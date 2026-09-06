use std::fmt;
use std::io::{self, Read, Write};

pub const MAGIC: [u8; 4] = *b"FRGE";
pub const VERSION: u8 = 1;
pub const MAX_FRAME_PAYLOAD: u32 = 16 * 1024 * 1024;

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
pub struct TaskRequest {
    pub task_id: u64,
    pub command: String,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    pub task_id: u64,
    pub success: bool,
    pub timed_out: bool,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub worker_id: String,
    pub unix_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidMagic,
    UnsupportedVersion(u8),
    UnknownMessage(u8),
    FrameTooLarge(u32),
    InvalidPayload(&'static str),
    InvalidUtf8,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "invalid frame magic"),
            Self::UnsupportedVersion(version) => write!(formatter, "unsupported protocol version {version}"),
            Self::UnknownMessage(kind) => write!(formatter, "unknown message kind {kind}"),
            Self::FrameTooLarge(length) => write!(formatter, "frame payload too large: {length} bytes"),
            Self::InvalidPayload(message) => write!(formatter, "invalid payload: {message}"),
            Self::InvalidUtf8 => write!(formatter, "payload contains invalid UTF-8"),
        }
    }
}

impl std::error::Error for ProtocolError {}

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
        if self.payload.len() > MAX_FRAME_PAYLOAD as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame payload exceeds protocol limit"));
        }
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
        if length > MAX_FRAME_PAYLOAD {
            return Err(Box::new(ProtocolError::FrameTooLarge(length)));
        }

        let kind = MessageKind::from_byte(header[1])?;
        let mut payload = vec![0u8; length as usize];
        reader.read_exact(&mut payload)?;
        Ok(Self { kind, payload })
    }
}

fn put_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

fn put_i32(buffer: &mut Vec<u8>, value: i32) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtocolError> {
    let length = u32::try_from(bytes.len()).map_err(|_| ProtocolError::InvalidPayload("field too large"))?;
    put_u32(buffer, length);
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn put_string(buffer: &mut Vec<u8>, value: &str) -> Result<(), ProtocolError> {
    put_bytes(buffer, value.as_bytes())
}

fn read_exact<'a>(payload: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8], ProtocolError> {
    let end = offset.checked_add(length).ok_or(ProtocolError::InvalidPayload("offset overflow"))?;
    if end > payload.len() {
        return Err(ProtocolError::InvalidPayload("truncated payload"));
    }
    let bytes = &payload[*offset..end];
    *offset = end;
    Ok(bytes)
}

fn read_u32(payload: &[u8], offset: &mut usize) -> Result<u32, ProtocolError> {
    let bytes = read_exact(payload, offset, 4)?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("length checked")))
}

fn read_u64(payload: &[u8], offset: &mut usize) -> Result<u64, ProtocolError> {
    let bytes = read_exact(payload, offset, 8)?;
    Ok(u64::from_be_bytes(bytes.try_into().expect("length checked")))
}

fn read_i32(payload: &[u8], offset: &mut usize) -> Result<i32, ProtocolError> {
    let bytes = read_exact(payload, offset, 4)?;
    Ok(i32::from_be_bytes(bytes.try_into().expect("length checked")))
}

fn read_bytes<'a>(payload: &'a [u8], offset: &mut usize) -> Result<&'a [u8], ProtocolError> {
    let length = read_u32(payload, offset)? as usize;
    read_exact(payload, offset, length)
}

fn read_string(payload: &[u8], offset: &mut usize) -> Result<String, ProtocolError> {
    let bytes = read_bytes(payload, offset)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError::InvalidUtf8)
}

fn ensure_consumed(payload: &[u8], offset: usize) -> Result<(), ProtocolError> {
    if offset == payload.len() {
        Ok(())
    } else {
        Err(ProtocolError::InvalidPayload("trailing bytes"))
    }
}

impl TaskRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut payload = Vec::new();
        put_u64(&mut payload, self.task_id);
        put_string(&mut payload, &self.command)?;
        match self.timeout_ms {
            Some(timeout) => {
                payload.push(1);
                put_u64(&mut payload, timeout);
            }
            None => payload.push(0),
        }
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ProtocolError> {
        let mut offset = 0;
        let task_id = read_u64(payload, &mut offset)?;
        let command = read_string(payload, &mut offset)?;
        let timeout_ms = match read_exact(payload, &mut offset, 1)?[0] {
            0 => None,
            1 => Some(read_u64(payload, &mut offset)?),
            _ => return Err(ProtocolError::InvalidPayload("invalid timeout flag")),
        };
        ensure_consumed(payload, offset)?;
        Ok(Self { task_id, command, timeout_ms })
    }
}

impl TaskResult {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut payload = Vec::new();
        put_u64(&mut payload, self.task_id);
        payload.push(u8::from(self.success));
        payload.push(u8::from(self.timed_out));
        match self.exit_code {
            Some(code) => {
                payload.push(1);
                put_i32(&mut payload, code);
            }
            None => payload.push(0),
        }
        put_string(&mut payload, &self.stdout)?;
        put_string(&mut payload, &self.stderr)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ProtocolError> {
        let mut offset = 0;
        let task_id = read_u64(payload, &mut offset)?;
        let success = match read_exact(payload, &mut offset, 1)?[0] {
            0 => false,
            1 => true,
            _ => return Err(ProtocolError::InvalidPayload("invalid success flag")),
        };
        let timed_out = match read_exact(payload, &mut offset, 1)?[0] {
            0 => false,
            1 => true,
            _ => return Err(ProtocolError::InvalidPayload("invalid timeout flag")),
        };
        let exit_code = match read_exact(payload, &mut offset, 1)?[0] {
            0 => None,
            1 => Some(read_i32(payload, &mut offset)?),
            _ => return Err(ProtocolError::InvalidPayload("invalid exit-code flag")),
        };
        let stdout = read_string(payload, &mut offset)?;
        let stderr = read_string(payload, &mut offset)?;
        ensure_consumed(payload, offset)?;
        Ok(Self { task_id, success, timed_out, exit_code, stdout, stderr })
    }
}

impl Heartbeat {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut payload = Vec::new();
        put_string(&mut payload, &self.worker_id)?;
        put_u64(&mut payload, self.unix_seconds);
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ProtocolError> {
        let mut offset = 0;
        let worker_id = read_string(payload, &mut offset)?;
        let unix_seconds = read_u64(payload, &mut offset)?;
        ensure_consumed(payload, offset)?;
        Ok(Self { worker_id, unix_seconds })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let frame = Frame { kind: MessageKind::Heartbeat, payload: b"worker-1".to_vec() };
        let mut bytes = Vec::new();
        frame.encode(&mut bytes).unwrap();
        let decoded = Frame::decode(&mut bytes.as_slice()).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn task_request_round_trip_with_timeout() {
        let request = TaskRequest { task_id: 42, command: "echo forge".into(), timeout_ms: Some(1500) };
        let encoded = request.encode().unwrap();
        assert_eq!(TaskRequest::decode(&encoded).unwrap(), request);
    }

    #[test]
    fn task_request_round_trip_without_timeout() {
        let request = TaskRequest { task_id: 42, command: "echo forge".into(), timeout_ms: None };
        let encoded = request.encode().unwrap();
        assert_eq!(TaskRequest::decode(&encoded).unwrap(), request);
    }

    #[test]
    fn task_result_round_trip() {
        let result = TaskResult { task_id: 42, success: false, timed_out: true, exit_code: None, stdout: "out".into(), stderr: "task timed out".into() };
        let encoded = result.encode().unwrap();
        assert_eq!(TaskResult::decode(&encoded).unwrap(), result);
    }

    #[test]
    fn heartbeat_round_trip() {
        let heartbeat = Heartbeat { worker_id: "worker-1".into(), unix_seconds: 1234 };
        let encoded = heartbeat.encode().unwrap();
        assert_eq!(Heartbeat::decode(&encoded).unwrap(), heartbeat);
    }

    #[test]
    fn malformed_payload_is_rejected() {
        assert_eq!(TaskRequest::decode(&[0, 0, 0]).unwrap_err(), ProtocolError::InvalidPayload("truncated payload"));
    }
}
