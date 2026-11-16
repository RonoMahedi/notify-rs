// sqlx::migrate! embeds ./migrations at compile time. Without this, adding a new
// migration file doesn't trigger a rebuild and the binary silently ships without it.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
