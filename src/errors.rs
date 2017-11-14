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

        //TODO add UnexpectedPrimitiveBlock
    }
}
