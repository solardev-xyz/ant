//! Developer task runner. For now it only knows how to cross-compile
//! `ant-ffi` for the iOS simulator — the smoke-test app in
//! `examples/ios-download/` links the resulting static library.
//!
//! Usage:
//!
//!     cargo xtask build-ios-sim           # aarch64-apple-ios-sim (Apple Silicon)
//!     cargo xtask build-ios-sim --arch x86_64   # x86_64-apple-ios (Intel simulator)
//!     cargo xtask build-ios-sim --profile debug
//!
//! On success prints the absolute path of `libant_ffi.a`. The Xcode
//! project's "Build Rust" run-script phase calls this so a plain
//! ⌘B on the app re-cross-compiles the Rust side.
//!
//! This stays in the xtask crate (no `cargo-*` sub-commands, no
//! `just`) so the repo is buildable with nothing but a stable Rust
//! toolchain.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use anyhow::{anyhow, bail, Context, Result};

const DEFAULT_SIM_TARGET: &str = "aarch64-apple-ios-sim";
const X86_SIM_TARGET: &str = "x86_64-apple-ios";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xtask: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let sub = args
        .next()
        .ok_or_else(|| anyhow!("missing subcommand (try `build-ios-sim`)"))?;
    match sub.to_string_lossy().as_ref() {
        "build-ios-sim" => build_ios_sim(args.collect::<Vec<_>>()),
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => bail!("unknown subcommand: {other}"),
    }
}

fn print_help() {
    println!(
        "xtask — developer task runner\n\
         \n\
         SUBCOMMANDS:\n\
             build-ios-sim [--arch aarch64|x86_64] [--profile debug|release]\n\
                 Cross-compile crates/ant-ffi for the iOS simulator.\n\
                 Default: --arch aarch64 --profile release.\n"
    );
}

struct BuildOpts {
    target: &'static str,
    profile: Profile,
}

#[derive(Clone, Copy)]
enum Profile {
    Debug,
    Release,
}

impl Profile {
    fn as_str(self) -> &'static str {
        match self {
            Profile::Debug => "debug",
            Profile::Release => "release",
        }
    }
}

fn build_ios_sim(args: Vec<OsString>) -> Result<()> {
    let opts = parse_build_opts(args)?;

    ensure_rust_target(opts.target)?;

    let workspace_root = workspace_root()?;
    let mut cmd = Command::new(cargo_bin());
    cmd.arg("build")
        .arg("-p")
        .arg("ant-ffi")
        .arg("--target")
        .arg(opts.target)
        .current_dir(&workspace_root);
    if matches!(opts.profile, Profile::Release) {
        cmd.arg("--release");
    }
    // Xcode sets DEVELOPER_DIR / SDKROOT / IPHONEOS_DEPLOYMENT_TARGET
    // when driving xtask from a build phase. Propagating them by
    // default (no env scrubbing here) is what makes the script-phase
    // cross-compile work out of the box.

    let status = cmd
        .status()
        .with_context(|| format!("spawn `cargo build --target {}`", opts.target))?;
    if !status.success() {
        bail!("cargo build --target {} failed", opts.target);
    }

    let lib = workspace_root
        .join("target")
        .join(opts.target)
        .join(opts.profile.as_str())
        .join("libant_ffi.a");
    if !lib.exists() {
        bail!("expected {} after build, but it is missing", lib.display());
    }
    println!("{}", lib.display());
    Ok(())
}

fn parse_build_opts(args: Vec<OsString>) -> Result<BuildOpts> {
    let mut target = DEFAULT_SIM_TARGET;
    let mut profile = Profile::Release;
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let arg = arg.to_string_lossy().into_owned();
        match arg.as_str() {
            "--arch" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--arch requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                target = match v.as_str() {
                    "aarch64" => DEFAULT_SIM_TARGET,
                    "x86_64" => X86_SIM_TARGET,
                    other => bail!("unsupported --arch {other} (try aarch64 or x86_64)"),
                };
            }
            "--profile" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--profile requires a value"))?
                    .to_string_lossy()
                    .into_owned();
                profile = match v.as_str() {
                    "debug" => Profile::Debug,
                    "release" => Profile::Release,
                    other => bail!("unsupported --profile {other} (try debug or release)"),
                };
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(BuildOpts { target, profile })
}

/// Check that `rustup target list --installed` mentions the target we
/// need, and if not, hint at the one-shot fix. We don't auto-add the
/// target because that would surprise operators on restricted networks
/// where `rustup` can't hit `static.rust-lang.org`.
fn ensure_rust_target(target: &str) -> Result<()> {
    let out = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output();
    let Ok(out) = out else {
        // rustup missing — assume the operator managed their toolchain
        // some other way (distro package, manual build). Let the
        // subsequent `cargo build` fail loudly if the target is
        // actually unavailable.
        return Ok(());
    };
    if !out.status.success() {
        return Ok(());
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    if stdout.lines().any(|l| l.trim() == target) {
        return Ok(());
    }
    bail!(
        "rust target `{target}` is not installed. Install it with:\n\
         \n\
             rustup target add {target}\n"
    );
}

fn cargo_bin() -> OsString {
    env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"))
}

fn workspace_root() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .parent()
        .ok_or_else(|| anyhow!("xtask crate has no parent directory"))?;
    let root_manifest = root.join("Cargo.toml");
    if !root_manifest.exists() {
        bail!(
            "expected workspace root at {} but no Cargo.toml there",
            root.display()
        );
    }
    Ok(absolutise(root))
}

fn absolutise(p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(p)
    }
}
