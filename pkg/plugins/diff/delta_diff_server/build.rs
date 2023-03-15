fn main() {
    tonic_build::configure()
        .out_dir("./src")
        .build_client(false)
        .compile(
            &["../table_diff.proto"],
            &["../"],
        ).unwrap();
}

