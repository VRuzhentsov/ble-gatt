//! Repository tasks, run with `cargo xtask <task>` (the alias is in
//! `.cargo/config.toml`). This is the `xtask` pattern:
//! <https://github.com/matklad/cargo-xtask>.
//!
//! - `cargo xtask setup-hooks`: use the hooks in `.githooks/` for this clone.
//! - `cargo xtask pre-commit`: the checks that `.githooks/pre-commit` runs on
//!   the staged `.rs` files.
//!   - rustfmt: the staged content of each file must be formatted.
//!   - clippy: `cargo clippy` checks the whole workspace, but only warnings on
//!     the lines that `git diff --cached -U0` shows as added or changed stop
//!     the commit. Old warnings on other lines do not. Compile errors always
//!     stop it.

use std::collections::{HashMap, HashSet};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output, Stdio};

use serde_json::Value;

type Result<T> = std::result::Result<T, String>;

/// The clippy run. Lint levels come from `[workspace.lints]` in Cargo.toml.
const CLIPPY_ARGS: &[&str] = &[
    "clippy",
    "--workspace",
    "--all-targets",
    "--all-features",
    "--message-format=json",
];

fn main() -> ExitCode {
    let result = match std::env::args().nth(1).as_deref() {
        Some("pre-commit") => pre_commit(),
        Some("setup-hooks") => setup_hooks(),
        _ => {
            eprintln!("usage: cargo xtask <pre-commit|setup-hooks>");
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            eprintln!("xtask: {message}");
            ExitCode::from(2)
        }
    }
}

/// Runs `command` and returns its output. Exits with a clear message when
/// the program is not on PATH.
fn run(command: &mut Command, stdin: Option<&[u8]>) -> Result<Output> {
    let program = command.get_program().to_string_lossy().into_owned();
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    let mut child = match command.stdout(Stdio::piped()).spawn() {
        Ok(child) => child,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            eprintln!("pre-commit: \"{program}\" is not on PATH, so the Rust checks cannot run.");
            eprintln!("pre-commit: install the Rust toolchain (rustup, with the clippy and rustfmt components),");
            eprintln!("pre-commit: or skip the checks one time with \"git commit --no-verify\".");
            std::process::exit(1);
        }
        Err(error) => return Err(format!("cannot run {program}: {error}")),
    };
    if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(input)
            .map_err(|error| format!("cannot write to {program}: {error}"))?;
    }
    child
        .wait_with_output()
        .map_err(|error| format!("cannot run {program}: {error}"))
}

fn git(args: &[&str]) -> Result<String> {
    let output = run(Command::new("git").args(args).stderr(Stdio::piped()), None)?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn repo_root() -> Result<PathBuf> {
    Ok(PathBuf::from(
        git(&["rev-parse", "--show-toplevel"])?.trim_end(),
    ))
}

fn setup_hooks() -> Result<bool> {
    git(&["config", "core.hooksPath", ".githooks"])?;
    println!("Git now runs the hooks in .githooks/ for this clone.");
    Ok(true)
}

fn pre_commit() -> Result<bool> {
    let root = repo_root()?;
    std::env::set_current_dir(&root).map_err(|error| format!("cannot enter {root:?}: {error}"))?;
    let files: Vec<String> = git(&[
        "diff",
        "--cached",
        "--name-only",
        "-z",
        "--diff-filter=ACMR",
        "--",
        "*.rs",
    ])?
    .split('\0')
    .filter(|file| !file.is_empty())
    .map(str::to_owned)
    .collect();
    if files.is_empty() {
        return Ok(true);
    }
    // Run both, so that one commit attempt shows every problem.
    let formatted = fmt(&root, &files)?;
    let linted = clippy(&root, &files)?;
    Ok(formatted && linted)
}

/// The edition in the nearest Cargo.toml above `file` that sets one
/// (`edition.workspace = true` leads up to the workspace's own).
fn edition(root: &Path, file: &str) -> String {
    let mut dir = root.join(file);
    while dir.pop() && dir.starts_with(root) {
        let Ok(manifest) = std::fs::read_to_string(dir.join("Cargo.toml")) else {
            continue;
        };
        for line in manifest.lines() {
            let Some(rest) = line.trim().strip_prefix("edition") else {
                continue;
            };
            if let Some(value) = rest.trim_start().strip_prefix('=') {
                return value.trim().trim_matches('"').to_owned();
            }
        }
    }
    "2021".to_owned()
}

/// rustfmt reads the staged content from stdin, so it checks only that file,
/// not the modules it declares with `mod`. With stdin, `--check` prints the
/// diff but exits 0, so compare the formatted output with the staged content.
fn fmt(root: &Path, files: &[String]) -> Result<bool> {
    let mut ok = true;
    for file in files {
        let staged = run(
            Command::new("git")
                .args(["show", &format!(":{file}")])
                .stderr(Stdio::piped()),
            None,
        )?;
        if !staged.status.success() {
            continue;
        }
        let edition = edition(root, file);
        let args = ["--edition", edition.as_str()];
        let formatted = run(
            Command::new("rustfmt").args(args).stderr(Stdio::piped()),
            Some(&staged.stdout),
        )?;
        if formatted.status.success() && formatted.stdout == staged.stdout {
            continue;
        }
        ok = false;
        eprintln!(
            "rustfmt: {file} is not formatted (run \"rustfmt {} {file}\"):",
            args.join(" ")
        );
        let check = run(
            Command::new("rustfmt")
                .arg("--check")
                .args(args)
                .stderr(Stdio::piped()),
            Some(&staged.stdout),
        )?;
        eprint!(
            "{}{}",
            String::from_utf8_lossy(&check.stdout).replace("<stdin>", file),
            String::from_utf8_lossy(&check.stderr)
        );
    }
    Ok(ok)
}

/// Added or changed line ranges of each staged file, from the zero-context
/// diff.
fn changed_lines(files: &[String]) -> Result<HashMap<String, Vec<(u64, u64)>>> {
    let mut args = vec![
        "-c",
        "core.quotePath=false",
        "diff",
        "--cached",
        "-U0",
        "--no-color",
        "--no-ext-diff",
        "--src-prefix=a/",
        "--dst-prefix=b/",
        "--diff-filter=ACMR",
        "--",
    ];
    args.extend(files.iter().map(String::as_str));
    let mut changed: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
    let mut current: Option<String> = None;
    for line in git(&args)?.lines() {
        if let Some(target) = line.strip_prefix("+++ ") {
            current = target
                .trim_end_matches('\t')
                .strip_prefix("b/")
                .map(str::to_owned);
            continue;
        }
        let (Some(file), Some(hunk)) = (&current, line.strip_prefix("@@ -")) else {
            continue;
        };
        // `@@ -a[,b] +c[,d] @@`: the new lines are c ..= c + d - 1.
        let Some(new) = hunk
            .split(' ')
            .nth(1)
            .and_then(|part| part.strip_prefix('+'))
        else {
            continue;
        };
        let mut numbers = new.split(',').map(str::parse::<u64>);
        let (Some(Ok(start)), count) = (numbers.next(), numbers.next()) else {
            continue;
        };
        let count = match count {
            Some(Ok(count)) => count,
            _ => 1,
        };
        if count > 0 {
            changed
                .entry(file.clone())
                .or_default()
                .push((start, start + count - 1));
        }
    }
    Ok(changed)
}

/// `path` relative to `root`, with `/` separators. Cargo gives workspace
/// files relative to the workspace root, which is the repository root.
fn repo_path(root: &Path, path: &str) -> Option<String> {
    let path = Path::new(path);
    let relative = if path.is_absolute() {
        path.strip_prefix(root).ok()?
    } else {
        path
    };
    let parts: Vec<_> = relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect();
    Some(parts.join("/"))
}

fn clippy(root: &Path, files: &[String]) -> Result<bool> {
    let changed = changed_lines(files)?;
    eprintln!("clippy: checking the workspace (only warnings on staged lines stop the commit)");
    let output = run(
        Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned()))
            .args(CLIPPY_ARGS)
            .stderr(Stdio::inherit()),
        None,
    )?;

    let mut reported = HashSet::new();
    let (mut errors, mut on_changed_lines, mut elsewhere) = (0, 0, 0);
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] != "compiler-message" {
            continue;
        }
        let diagnostic = &message["message"];
        let level = diagnostic["level"].as_str().unwrap_or_default();
        let rendered = diagnostic["rendered"].as_str().unwrap_or_default();
        if !reported.insert(rendered.to_owned()) {
            continue;
        }
        if level.starts_with("error") {
            errors += 1;
            eprint!("{rendered}");
            continue;
        }
        let spans = diagnostic["spans"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default();
        if level != "warning" || spans.is_empty() {
            continue;
        }
        let hit = spans
            .iter()
            .filter(|span| span["is_primary"] == true)
            .any(|span| {
                let file = span["file_name"]
                    .as_str()
                    .and_then(|name| repo_path(root, name));
                let (Some(start), Some(end)) =
                    (span["line_start"].as_u64(), span["line_end"].as_u64())
                else {
                    return false;
                };
                file.and_then(|file| changed.get(&file))
                    .is_some_and(|ranges| {
                        ranges.iter().any(|&(from, to)| start <= to && end >= from)
                    })
            });
        if hit {
            on_changed_lines += 1;
            eprint!("{rendered}");
        } else {
            elsewhere += 1;
        }
    }

    if errors > 0 || (!output.status.success() && on_changed_lines == 0) {
        eprintln!(
            "clippy: the workspace does not compile (cargo exit code {:?}).",
            output.status.code()
        );
        Ok(false)
    } else if on_changed_lines > 0 {
        eprintln!(
            "clippy: {on_changed_lines} warning(s) on staged lines. Fix them, or use #[allow(...)] with a reason."
        );
        Ok(false)
    } else {
        eprintln!("clippy: no warnings on staged lines ({elsewhere} warning(s) on other lines were ignored).");
        Ok(true)
    }
}
