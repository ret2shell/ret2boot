# AGENTS

## Quick Start

- Read `src/main.rs` first to understand the runtime order: CLI parse -> telemetry -> config load -> startup checks -> installer workflow.
- Read `src/startup.rs` next for locale selection, safety confirmation, and privilege acquisition.
- Read `src/update.rs` when changing startup connectivity or self-update behavior. Release-source integration starts there.
- Read `src/config.rs` before changing installer behavior. All questionnaire answers, review state, and execution progress are persisted there for resume support.
- Read `src/install/mod.rs` for the high-level flow: questionnaire -> plan review -> installation phase journal.
- Read `src/install/steps/mod.rs` before adding or changing any atomic installation step. This is the step registry and the trait contract for collect/describe/install/uninstall/rollback.

## Project Layout

- `Cargo.toml`: crate manifest and runtime dependencies.
- `rustfmt.toml`, `taplo.toml`: formatting rules for Rust and TOML.
- `src/main.rs`: application entrypoint.
- `src/cli.rs`: clap CLI definition.
- `src/startup.rs`: terminal capability checks, locale selection, safety prompts, and privilege session setup.
- `src/update.rs`: release-source abstraction, GitHub release checks, and downloaded update staging.
- `src/config.rs`: persisted installer config, questionnaire answers, review status, and execution journal.
- `src/install/mod.rs`: installer orchestration and plan/review/progress output.
- `src/install/collectors/`: reusable terminal question helpers.
- `src/install/steps/mod.rs`: atomic installation step trait, contexts, and registry.
- `src/privilege.rs`: privilege escalation backends and keepalive behavior.
- `src/l10n.rs`: locale normalization and language option helpers.
- `src/resources.rs`: embedded resource access.
- `src/resources/locales/`: localized strings.
- `src/resources/templates/`: embedded text templates.
- `src/telemetry.rs`: tracing initialization and `RET2SHELL_LOG` support.
- `src/terminal.rs`: terminal charset detection.
- `src/ui.rs`: clap/cargo-style terminal output helpers.
- `target/`: build artifacts; ignore for code navigation and edits.

## Working Rules

- During questionnaire work, do not perform real installation side effects. Collect data only.
- Persist every questionnaire answer immediately after it changes so interrupted sessions can resume safely.
- When a stored answer already exists, use it as the default for the next prompt whenever possible.
- Model every deployable unit as an atomic step that can also uninstall or roll back itself cleanly.
- Register new atomic steps in `src/install/steps/mod.rs` and keep plan descriptions, install logic, and uninstall logic together.

## Quality Gates

- After every completed unit of work, run `cargo +nightly fmt`.
- After formatting, run `cargo clippy`.
- Do not leave any formatter diffs, clippy warnings, or clippy errors unresolved.
- If either command changes files or reports problems, fix the code and rerun both commands until clean.

## Commit Workflow

- After each completed step, create a git commit.
- Use gitmoji-style commit titles.
- Keep the summary portion lowercase.
- Write a bullet-list body describing the concrete changes.
- Example title format: `:sparkles: add install step journal`
- Example body format:
  - `- persist questionnaire answers immediately`
  - `- add atomic install step trait skeleton`
