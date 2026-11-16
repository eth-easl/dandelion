# dandelion CLI

A command-line client for the Dandelion runtime API. It registers functions and compositions,
invokes them and shows the results. It can also check whether a composition parses, using the same
parser as the server, without contacting a server.

The CLI speaks the server's frontend protocol (`[version][Protobuf metadata][payload]`). Its
metadata types are generated from the server's own `server/proto/protocol.proto`, and the tests in
the `server_compat` tests in `src/protocol.rs` run requests and responses through the server's encoder and
decoders, so the two stay in sync.

## Install

From the repository root:

```sh
cargo install --path cli
```

This installs the `dandelion` binary. Building needs `protoc`, like the rest of the workspace.

### Shell completions

`dandelion completions <SHELL>` prints a completion script. The steps are the same on Linux and
macOS.

**bash** needs the `bash-completion` package (version 2). Most Linux distros include it; on macOS,
install it with `brew install bash-completion@2`, which also needs a newer bash than the one macOS
ships. Scripts in this folder are then loaded automatically:

```sh
mkdir -p ~/.local/share/bash-completion/completions
dandelion completions bash > ~/.local/share/bash-completion/completions/dandelion
```

**zsh**: save the script to a folder in your `fpath`:

```sh
mkdir -p ~/.zfunc
dandelion completions zsh > ~/.zfunc/_dandelion
```

Then add this to `~/.zshrc`, before any existing `compinit` line:

```sh
fpath=(~/.zfunc $fpath)
autoload -Uz compinit && compinit
```

Open a new shell for the completions to take effect. If zsh still doesn't complete `dandelion`,
clear its cache with `rm -f ~/.zcompdump*` and open a new shell again.

## Quick start

```sh
# Point the CLI at a server and set defaults for function registration
dandelion config profile add local --url http://localhost:8080
dandelion config set engine Kvm
dandelion config set context_size 64MiB

# Check that the server is reachable
dandelion ping

# Register a function and invoke it
dandelion function register ./target/matmul --input-set matrices --output-set result
dandelion invoke matmul --input matrices:a=@a.bin --input matrices:b=@b.bin
```

## Commands

### Register a function

```sh
dandelion function register <BINARY> [--name N] [--engine Process|Kvm|Cheri] [--context-size 64M] \
    [--input-set NAME]... [--static SET:ITEM[#KEY]=@FILE]... [--output-set NAME]... [--min-set-bytes N]...
```

- `--name` defaults to the file name of the binary without its extension.
- `--engine` and `--context-size` default to the values in the config.
- `--server-path /opt/fns/matmul` uses a binary that is already on the server instead of
  uploading `<BINARY>`. You can give one or the other, not both.
- `--static` attaches a file to a declared input set. The server passes it to the function on
  every invocation. `KEY` defaults to 0.

You can also describe the function in a TOML manifest. Paths in it are relative to the manifest
file, and command-line flags override its values:

```toml
name = "matmul"
engine = "Kvm"
context_size = "64M"
binary = "./target/matmul"
output_sets = ["result"]

[[input_sets]]
name = "matrices"

[[input_sets]]
name = "config"
static = [{ item = "params", file = "./params.bin" }]   # optional `key = N`
```

A manifest sets either `binary` or `server_path`. A `<BINARY>` or `--server-path` flag replaces
whichever the manifest sets.

```sh
dandelion function register --manifest matmul.toml
```

### Register a composition

```sh
dandelion composition register compositions.txt
cat compositions.txt | dandelion composition register -
```

### Check a composition

```sh
dandelion composition check compositions.txt
cat compositions.txt | dandelion composition check
```

This parses the compositions locally with the server's parser and prints each composition with its
number of function applications, or the parse error with its location. Function declarations are
not checked against registered functions, only whether the compositions themselves are valid. It
needs no server and ignores the config. (This replaces the former `dparser` binary of the
`composition` crate.)

### Invoke

```sh
dandelion invoke <NAME> [--input SET:ITEM[#KEY]=@FILE | --input SET:ITEM[#KEY]=TEXT]...
dandelion invoke --composition <FILE | -> ...
```

- **Inputs:** sets are sent in the order they first appear, and items in the order given. `KEY`
  is an unsigned 32-bit integer and defaults to 0.
- **`--input-dir DIR`:** reads inputs from a directory instead. Each subdirectory is a set and
  each file in it is an item named `ITEM[#KEY]`. Both are read in name order, and hidden files
  are skipped.
- **`--no-cache`:** uses the cold path instead of the hot one.

How the result is shown:

| Option | Output |
|---|---|
| (none) | Summary of each set and item; short text items and the trace are shown inline |
| `--json` | `{ "sets": [...], "trace": ... }`, with item data base64-encoded |
| `--out-dir DIR` | Each item written to `DIR/<set index>/<item>`, the trace to `DIR/trace.json` |
| `--raw SET:ITEM` | Only that item's bytes, for piping. `SET` is the set index; `ITEM` is the identifier or index |

The trace is only sent by servers built with the `trace-timestamps` or `trace-data` feature.

### Trace timestamps

```sh
dandelion trace timestamps [--json]
```

Lists the names of the timestamps the server records in a trace, from `GET /trace/timestamps`. The
`ts` array in an invocation's trace has one value (in microseconds) per name, in the same order,
so each name is printed with its index. `--json` prints the names as a JSON array instead.

### Ping

`dandelion ping` sends `GET /` and prints the status and how long the request took. It also checks
that the answer is a status message in this client's protocol version, so it fails if something
other than a compatible Dandelion server is listening.

## Configuration

The config file is `~/.config/dandelion/config.toml`, or `$XDG_CONFIG_HOME/dandelion/config.toml`
if that variable is set. You can set `DANDELION_CONFIG` to use a different file, and
`dandelion config path` prints the location in use.

```toml
active_profile = "local"

[profiles.local]
url = "http://localhost:8080"
engine = "Process"
context_size = "64M"
timeout = "2m"

[profiles.remote]
url = "http://dandelion-host:8080"
engine = "Kvm"
```

| Key | Meaning | Default |
|---|---|---|
| `url` | Server URL | `http://localhost:8080` |
| `engine` | Default engine for `function register` | none |
| `context_size` | Default context size for `function register` | none |
| `timeout` | Request timeout, e.g. `30s` or `5m` | none (connecting has its own 10s limit) |
| `hot_path` | Invoke path | `/hot/matmul` |
| `cold_path` | Invoke path for `--no-cache` | `/cold/matmul` |

```sh
dandelion config get [KEY]          # one key, or all keys of the active profile
dandelion config set KEY VALUE
dandelion config unset KEY
dandelion config list               # all profiles
dandelion config profile list | add NAME [--url URL] | use NAME | remove NAME
```

`get`, `set` and `unset` act on the active profile, or on the profile named by `--profile`.

Settings are taken from the first place that has them:

1. command-line flag (`--url`, `--profile`, `--timeout`)
2. environment variable (`DANDELION_URL`, `DANDELION_PROFILE`)
3. the active profile
4. the built-in default

## Global options

`--url`, `--profile`, `--timeout <DURATION>`, and `-v/--verbose`, which prints the request, the
status, the sizes and the timing to stderr.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Local or usage error |
| 2 | Connection failure or timeout |
| 3 | Server returned 400 (or another 4xx) |
| 4 | Server returned 500 (or another 5xx) |
| 5 | Server returned 503 (shutting down) |

Server error messages (`Failed to serve request: …`) are printed to stderr.

## Development

```sh
cargo test -p dandelion_cli     # unit tests, end-to-end tests against a mock server (httpmock),
                                # and compatibility tests against the server's protocol code
cargo clippy -p dandelion_cli --all-targets
```

The compatibility tests build the server crate, which needs OpenSSL. Without its development
headers, add `-F dandelion_server/vendored-ssl`.
