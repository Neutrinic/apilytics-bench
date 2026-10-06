#!/bin/bash
# Installs the synthetic API on a Debian or Ubuntu host as two systemd services, run by an
# unprivileged user from this checkout's server/ directory:
#   synthetic-rest-api        the Axum server, on loopback port 18601
#   synthetic-rest-toxiproxy  Toxiproxy, listening on SYNTHETIC_LISTEN_HOST:SYNTHETIC_PORT and
#                             forwarding to the server; it injects TCP resets for fault profiles
# The services aren't enabled at boot; start them with `control.py start`. A sudoers rule lets the
# user start, stop and query just these two services without a password.
#
# usage: sudo [SYNTHETIC_USER=…] [SYNTHETIC_LISTEN_HOST=…] [SYNTHETIC_PORT=…] [SYNTHETIC_PUBLIC_BASE=…] deploy/install.sh
#   SYNTHETIC_USER         the account that runs the services; defaults to the one that ran sudo
#   SYNTHETIC_LISTEN_HOST  the address clients connect to; defaults to the host's first address
#   SYNTHETIC_PORT         defaults to 18600
#   SYNTHETIC_PUBLIC_BASE  the URL clients use, for Link headers; defaults to http://host:port
# Generate fixtures afterwards with server/generate.py, using the same SYNTHETIC_PUBLIC_BASE.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "Run as root, with sudo" >&2; exit 1; }
root=$(cd "$(dirname "$0")/../server" && pwd)
user=${SYNTHETIC_USER:-${SUDO_USER:-}}
[ -n "$user" ] && [ "$user" != root ] || { echo "Set SYNTHETIC_USER to an unprivileged account" >&2; exit 1; }
host=${SYNTHETIC_LISTEN_HOST:-$(hostname -I | awk '{print $1}')}
port=${SYNTHETIC_PORT:-18600}
public=${SYNTHETIC_PUBLIC_BASE:-http://$host:$port}
toxiproxy_version=2.12.0
toxiproxy_sha256=556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc  # linux-amd64
as_user() { runuser -u "$user" -- "$@"; }

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -yq python3-venv build-essential curl

# Python tools, from the locked versions.
as_user python3 -m venv "$root/.venv"
as_user "$root/.venv/bin/pip" install -q -r "$root/requirements.lock"

# Rust, isolated under server/.rust at the version the server pins, then the release build.
toolchain=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$root/axum/rust-toolchain.toml")
if [ ! -x "$root/.rust/cargo/bin/cargo" ]; then
  as_user bash -c "mkdir -p '$root/.rust' && curl -fsSL https://sh.rustup.rs -o '$root/.rust/rustup-init.sh' &&
    RUSTUP_HOME='$root/.rust/rustup' CARGO_HOME='$root/.rust/cargo' sh '$root/.rust/rustup-init.sh' \
      -y --no-modify-path --profile minimal --default-toolchain '$toolchain'"
fi
as_user bash -c "cd '$root' && RUSTUP_HOME='$root/.rust/rustup' CARGO_HOME='$root/.rust/cargo' \
  '$root/.rust/cargo/bin/cargo' build --release --locked --manifest-path axum/Cargo.toml &&
  install -m 755 axum/target/release/synthetic-rest-axum ./synthetic-rest-axum"

# Toxiproxy, checked against its pinned hash.
if [ ! -x "$root/toxiproxy-server" ]; then
  as_user curl -fsSL -o "$root/toxiproxy-server" \
    "https://github.com/Shopify/toxiproxy/releases/download/v$toxiproxy_version/toxiproxy-server-linux-amd64"
fi
echo "$toxiproxy_sha256  $root/toxiproxy-server" | sha256sum -c -
chmod 755 "$root/toxiproxy-server"
as_user bash -c "printf '[\n  {\"name\":\"synthetic\",\"listen\":\"%s:%s\",\"upstream\":\"127.0.0.1:18601\",\"enabled\":true}\n]\n' '$host' '$port' > '$root/toxiproxy.json'"

cat > /etc/systemd/system/synthetic-rest-api.service <<UNIT
[Unit]
Description=Synthetic REST API (Axum)
After=network-online.target
[Service]
User=$user
Group=$(id -gn "$user")
WorkingDirectory=$root
Environment=SYNTHETIC_ROOT=$root SYNTHETIC_LISTEN=127.0.0.1:18601 SYNTHETIC_PUBLIC_BASE=$public
ExecStart=$root/synthetic-rest-axum
Restart=on-failure
RestartSec=3
LimitNOFILE=65536
TimeoutStopSec=20
UNIT
cat > /etc/systemd/system/synthetic-rest-toxiproxy.service <<UNIT
[Unit]
Description=Synthetic REST TCP fault proxy
After=network-online.target synthetic-rest-api.service
[Service]
User=$user
Group=$(id -gn "$user")
WorkingDirectory=$root
ExecStart=$root/toxiproxy-server -host 127.0.0.1 -port 18604 -config $root/toxiproxy.json -seed 42
ExecStartPost=$root/.venv/bin/python $root/control.py sync
Restart=on-failure
RestartSec=3
LimitNOFILE=65536
UNIT
systemctl daemon-reload

services='synthetic-rest-api.service synthetic-rest-toxiproxy.service'
printf '%s ALL=(root) NOPASSWD: /usr/bin/systemctl start %s, /usr/bin/systemctl stop %s, /usr/bin/systemctl status %s\n' \
  "$user" "$services" "$services" "$services" > /etc/sudoers.d/synthetic-rest
chmod 440 /etc/sudoers.d/synthetic-rest
visudo -cf /etc/sudoers.d/synthetic-rest

systemctl start synthetic-rest-api synthetic-rest-toxiproxy
curl --retry 10 --retry-connrefused --retry-delay 1 -fsS "http://$host:$port/health"; echo
echo "Synthetic API: $public"
