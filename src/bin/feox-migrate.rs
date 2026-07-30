use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;

use feoxdb::{migrate, MigrationError, MigrationOptions};

const HELP: &str = "\
Copy a FeOxDB format v1 or v2 store into a new format v3 store.

Usage:
  feox-migrate --source <PATH> --destination <PATH> [OPTIONS]

Options:
  --source <PATH>                         Offline v1 or v2 source store
  --destination <PATH>                    New v3 store; must not already exist
  --allow-ambiguous-legacy-recovery       Accept ambiguous legacy deletion markers
  -h, --help                              Print help
  -V, --version                           Print version

The source must not be open by another process. Migration reads the source
without modifying it and verifies the new destination before succeeding.
";

struct Args {
    source: PathBuf,
    destination: PathBuf,
    allow_ambiguous_legacy_recovery: bool,
}

enum Action {
    Migrate(Args),
    Help,
    Version,
}

fn main() -> ExitCode {
    let action = match parse_args(env::args_os().skip(1)) {
        Ok(action) => action,
        Err(error) => {
            eprintln!("error: {error}\n\n{HELP}");
            return ExitCode::from(2);
        }
    };

    let args = match action {
        Action::Help => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        Action::Version => {
            println!("feox-migrate {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Action::Migrate(args) => args,
    };

    if args.allow_ambiguous_legacy_recovery {
        eprintln!(
            "warning: ambiguous legacy recovery can interpret continuation data as live records; use only with a trusted backup"
        );
    }

    let options = MigrationOptions::new(args.source, args.destination)
        .allow_ambiguous_legacy_recovery(args.allow_ambiguous_legacy_recovery);
    match migrate(options) {
        Ok(report) => {
            println!(
                "migration complete: source_format=v{} destination_format=v{} records={} value_bytes={} destination_bytes={} ambiguous_markers={}",
                report.source_version,
                report.destination_version,
                report.records,
                report.value_bytes,
                report.destination_size,
                report.ambiguous_legacy_markers,
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("migration failed: {error}");
            if matches!(error, MigrationError::AmbiguousLegacyRecovery) {
                eprintln!(
                    "hint: rerun with --allow-ambiguous-legacy-recovery only after retaining a trusted backup"
                );
            }
            ExitCode::from(1)
        }
    }
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Action, String> {
    let mut source = None;
    let mut destination = None;
    let mut allow_ambiguous_legacy_recovery = false;
    let mut args = args.into_iter();

    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("-h" | "--help") => return Ok(Action::Help),
            Some("-V" | "--version") => return Ok(Action::Version),
            Some("--source") => {
                if source.is_some() {
                    return Err("--source was specified more than once".to_owned());
                }
                source = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| "--source requires a path".to_owned())?,
                ));
            }
            Some("--destination") => {
                if destination.is_some() {
                    return Err("--destination was specified more than once".to_owned());
                }
                destination = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| "--destination requires a path".to_owned())?,
                ));
            }
            Some("--allow-ambiguous-legacy-recovery") => {
                if allow_ambiguous_legacy_recovery {
                    return Err(
                        "--allow-ambiguous-legacy-recovery was specified more than once".to_owned(),
                    );
                }
                allow_ambiguous_legacy_recovery = true;
            }
            Some(argument) => return Err(format!("unknown option: {argument}")),
            None => return Err(format!("unknown non-UTF-8 option: {argument:?}")),
        }
    }

    Ok(Action::Migrate(Args {
        source: source.ok_or_else(|| "--source is required".to_owned())?,
        destination: destination.ok_or_else(|| "--destination is required".to_owned())?,
        allow_ambiguous_legacy_recovery,
    }))
}
