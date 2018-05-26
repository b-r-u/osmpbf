use protobuf::ProtobufError;
use std::io::Read;


pub(crate) fn parse_message_from_bytes<M>(bytes: &[u8]) -> Result<M, ProtobufError>
    where M: ::protobuf::Message
{
    let mut stream = ::protobuf::CodedInputStream::from_bytes(bytes);
    let mut message: M = ::protobuf::Message::new();
    message.merge_from(&mut stream)?;

    if message.is_initialized() {
        Ok(message)
    } else {
        Err(::protobuf::ProtobufError::message_not_initialized(""))
    }
}

pub(crate) fn parse_message_from_reader<R, M>(reader: &mut R) -> Result<M, ProtobufError>
    where R: Read,
          M: ::protobuf::Message,
{
    let mut stream = ::protobuf::CodedInputStream::new(reader);
    let mut message: M = ::protobuf::Message::new();
    message.merge_from(&mut stream)?;

    stream.check_eof()?;

    if message.is_initialized() {
        Ok(message)
    } else {
        Err(::protobuf::ProtobufError::message_not_initialized(""))
    }
}
