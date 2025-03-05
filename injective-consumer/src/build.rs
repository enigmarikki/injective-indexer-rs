use std::env;

fn main() {
    // Specify the path to your schema file.
    let schema_file = "src/flatbuf/stream_event.fbs";
    let out_dir = env::var("OUT_DIR").unwrap();

    // Invoke flatc to generate Rust code from the schema.
    // Make sure that the `flatc` command is in your PATH.
    let status = std::process::Command::new("flatc")
        .args(&["--rust", "-o", &out_dir, schema_file])
        .status()
        .expect("Failed to run flatc");

    assert!(status.success());
}
