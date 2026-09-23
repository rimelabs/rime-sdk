use prost::Message;

fn main() {
    println!("cargo:rerun-if-changed=schema.bin");
    let descriptor =
        prost_types::FileDescriptorSet::decode(include_bytes!("schema.bin").as_slice())
            .expect("pinned protocol descriptor");
    tonic_prost_build::configure()
        .compile_fds(descriptor)
        .expect("generate pinned Rime protocol");
}
