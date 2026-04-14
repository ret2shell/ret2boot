# Ret2Boot

Interactive installer and lifecycle management tool for [Ret2Shell](https://github.com/ret2shell/ret2shell) [(docs)](https://ret.sh.cn/) platform.

**Ret2Boot** is a Rust-based bootstrapping tool that automates the deployment and management of Ret2Shell clusters. It provides a comprehensive installation workflow with preflight validation, cluster initialization, and platform deployment.

> [!WARNING]
> This tool is in early work-in-progress development.
>
> You can refer to [DevOps Docs](https://ret.sh.cn/devops) for manual deployment instructions.

## Features

- **Interactive Installation**: Guided questionnaire-based installation with state persistence
- **Multi-language Support**: English, Japanese, Simplified Chinese, Traditional Chinese
- **Atomic Installation Steps**: Modular, rollback-compatible installation steps
- **Preflight Validation**: System requirements checking (disk, memory, kernel modules, network connectivity)
- **Cluster Management**:
  - Kubernetes cluster bootstrapping
  - Helm CLI setup and configuration
  - Application gateway deployment
  - Ret2Shell platform deployment
- **Lifecycle Commands**:
  - `install`: Fresh cluster deployment
  - `update`: Update existing deployments
  - `sync`: Synchronize configuration with running systems
  - `uninstall`: Clean removal of components
- **Privilege Escalation**: Automatic privilege acquisition when needed
- **Resumable Installation**: State tracking allows interrupted installations to resume

## System Requirements

### Minimum Resources
- **Disk Space**: More than 10 GB free (warning at <20 GB)
- **Memory**: More than 4 GB available (warning at <8 GB)

### Supported Platforms
- Linux with support for kernel modules (cgroup, netfilter)
- Package managers: apt (Debian/Ubuntu), yum/dnf (RHEL/Fedora), pacman (Arch)

## Installation

```bash
# Default: runs installation workflow
./ret2boot

# Specific commands
./ret2boot install     # New cluster installation
./ret2boot update      # Update existing deployment
./ret2boot sync        # Synchronize with running clusters
./ret2boot uninstall   # Remove components
```

## Installation Workflow

1. **Startup Checks**
   - Locale and language selection
   - Terminal capability detection
   - Privilege acquisition
   - Safety confirmations

2. **Questionnaire Phase**
   - Collect deployment requirements
   - Platform configuration
   - Networking setup
   - Persist answers for resume support

3. **Review Phase**
   - Display generated installation plan
   - Confirmation before execution

4. **Installation Steps**
   - Preflight validation (system resources, dependencies)
   - Cluster bootstrap
   - Helm CLI installation
   - Application gateway setup
   - Platform deployment
   - Worker platform probing

5. **Recovery**
   - Automatic failure tracking
   - State preservation at `~/.config/ret2boot/`
   - Resume capability after interruptions

## Configuration

Installation state and questionnaire answers are persisted in:
- **Linux/macOS**: `~/.config/ret2boot/config.toml`

Configuration includes:
- Selected language and locale
- Installation progress and completion status
- Questionnaire answers with review state
- Failure logs for debugging

## Build

```bash
cargo build --release
```

**Requirements**:
- Rust
- Cargo

## Development

See [AGENTS.md](./AGENTS.md) for development guidelines:
- Formatting: `cargo +nightly fmt`
- Linting: `cargo clippy`
- All code must pass formatter and clippy checks before commit
- Use gitmoji style commit messages

## License

GPL-3.0-only - See [LICENSE](./LICENSE)
