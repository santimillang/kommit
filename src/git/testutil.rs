use std::path::Path;
use std::process::Command;

/// Runs the real `git` CLI against a bare repository and returns stdout. Panics on failure.
pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("--git-dir")
        .arg(dir)
        .args(args)
        .output()
        .expect("git CLI must be installed for tests");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
