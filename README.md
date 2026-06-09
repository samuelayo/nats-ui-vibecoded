# NATS Observatory

A desktop NATS and JetStream inspector built with Go, Wails, and Vue.

Built with Codex.

The app is designed for scheduler-heavy JetStream setups, including sharded streams, scheduled messages, WorkQueue streams, consumers, stream browsing, Core NATS pub/sub, key-value buckets, and object stores.

## Features

- Connect to NATS with URL, username/password, token, or creds file.
- Save local connection profiles.
- View server, JetStream, stream, consumer, storage, and retry-pressure stats.
- Browse WorkQueue streams with deleted/gapped sequences.
- Inspect message headers and payloads.
- Republish an exact message payload and headers.
- View candidate messages around a consumer ack window.
- Subscribe and publish to Core NATS subjects.
- Inspect key-value buckets and object stores.

## Requirements

- Go 1.25 or newer.
- Node.js 20 or newer.
- Wails CLI v2. Install this before trying to build the desktop app.
- NATS CLI on your `PATH` if you want to use the in-app CLI page.
- A NATS server with JetStream enabled for JetStream views.

Install Wails if needed:

```powershell
go install github.com/wailsapp/wails/v2/cmd/wails@latest
```

On macOS/Linux:

```bash
go install github.com/wailsapp/wails/v2/cmd/wails@latest
```

## Setup

Clone the repository, then install frontend dependencies:

```powershell
cd nats-ui-wails
cd frontend
npm install
cd ..
```

On macOS/Linux:

```bash
cd nats-ui-wails
cd frontend
npm install
cd ..
```

## Development

Run the app in live development mode:

```powershell
wails dev
```

Wails starts the Go backend and the Vue frontend with hot reload.

## Build

Use Wails to produce desktop executables. Do not use `go build` directly for release binaries; Wails packages the Go backend, Vue frontend assets, app icon, and platform metadata together.

Windows:

```powershell
wails build -platform windows/amd64 -o nats-ui-wails.exe -ldflags "-linkmode=internal -H=windowsgui"
```

macOS:

```bash
wails build -platform darwin/universal
```

Linux:

```bash
wails build -platform linux/amd64
```

Build outputs are written under `build/bin`.

## Local Database

Connection profiles are stored in a local SQLite database. This database is created automatically on first run; it is not committed to git.

Default locations:

- Windows: `C:\Users\<you>\.nats-ui\nats-ui.db`
- macOS: `/Users/<you>/.nats-ui/nats-ui.db`
- Linux: `/home/<you>/.nats-ui/nats-ui.db`

The app creates the `.nats-ui` folder automatically with `os.MkdirAll`, so a fresh checkout should not fail because the database is missing.

The SQLite database is not currently encrypted. Anyone who can read `~/.nats-ui/nats-ui.db` can open it with a SQLite viewer and inspect saved profile data. Profile names, URLs, usernames, creds paths, tokens, and recoverable passwords should be treated as readable by someone with access to that file. Passwords are lightly obfuscated for local convenience, not protected as a security boundary. Avoid saving production credentials unless your machine/user profile is trusted.

## Git Hygiene

The repository ignores local runtime files such as:

- `build/bin`
- `node_modules`
- `frontend/dist`
- `.nats-ui/`
- `*.db`, `*.sqlite`, and WAL sidecar files
- `.env`, creds, JWT, and NKey files

Do not commit real NATS credentials, creds files, tokens, or copied profile databases.

## Project Layout

```text
.
|-- app.go              # Wails backend methods and NATS/JetStream logic
|-- db.go               # Local SQLite profile store
|-- main.go             # Wails app entrypoint
|-- frontend/           # Vue UI
|-- build/appicon.png   # App icon source
|-- build/windows/      # Windows packaging assets
`-- wails.json          # Wails project config
```

## Notes

- The Vue app is display and interaction only; NATS access stays in Go.
- The CLI page runs the installed `nats` binary with parsed arguments. It does not execute arbitrary shell commands. Commands that normally ask for confirmation, such as `stream purge`, need `--force` or the page's "force / no prompt" toggle.
- Saved profile secrets stay in the local SQLite DB and are not returned to the frontend profile list.
- For WorkQueue streams with gaps, message browsing probes available sequences and streams matches into the UI as they are found.
