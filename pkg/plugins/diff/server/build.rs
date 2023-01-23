fn main() {
    tonic_build::configure()
        // .build_client(false)
        .out_dir("./src")
        .build_client(false)
        .compile(
            &["../diff.proto"],
            &["../"],
        ).unwrap();
}
