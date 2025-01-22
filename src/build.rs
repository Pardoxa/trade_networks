use std::process::Command;

fn main() {
    let git_hash = match Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output(){
            Ok(output) => {
                match String::from_utf8(output.stdout){
                    Ok(stdout) => stdout,
                    Err(_) => "Git Hash parsing Error".to_owned()
                }
            },
            Err(_) => "Unable to call program git during compilation".to_owned()
    };
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
    println!("cargo:rustc-env=BUILD_TIME_CHRONO={}", chrono::offset::Local::now());
}