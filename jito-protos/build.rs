use tonic_build::configure;

fn main() {
    configure()
        .compile(
            &[
                "protos/shared.proto",
                "protos/packet.proto",
                "protos/relayer.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
