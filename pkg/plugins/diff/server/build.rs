fn main() {
    tonic_build::configure()
        // .build_client(false)
        .out_dir("./src")
        .compile(
            &["../diff.proto"],
            &["../"],
        ).unwrap();
}
