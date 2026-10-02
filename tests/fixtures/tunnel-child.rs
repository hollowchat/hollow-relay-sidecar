fn main() {
    std::fs::write(std::env::var("HOLLOW_TEST_CHILD_PID_FILE").unwrap(), std::process::id().to_string()).unwrap();
    println!("{{\"type\":\"ready\",\"publicUrl\":\"https://nonexistent-hollow-test.trycloudflare.com\"}}");
    loop {std::thread::sleep(std::time::Duration::from_secs(1));}
}
