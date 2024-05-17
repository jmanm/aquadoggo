use capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("capnp/aquadoggo.capnp")
        .run().expect("schema compiler command");
}