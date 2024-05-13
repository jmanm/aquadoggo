use capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/rpc")
        .file("capnp/rpc.capnp")
        .run().expect("schema compiler command");
}