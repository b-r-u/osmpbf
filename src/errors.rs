error_chain!{
    foreign_links {
        Io(::std::io::Error);
        Protobuf(::protobuf::ProtobufError);
    }

    errors {
        StringtableIndexOutOfBounds(index: usize) {
            description("stringtable index out of bounds")
            display("stringtable index out of bounds: {}", index)
        }

        BlobHeaderTooBig(size: u64) {
            description("blob header is too big")
            display("blob header is too big: {} bytes", size)
        }

        BlobMessageTooBig(size: u64) {
            description("blob message is too big")
            display("blob message is too big: {} bytes", size)
        }

        //TODO add UnexpectedPrimitiveBlock
    }
}
