#!/bin/bash
# Stops and removes the services and sudoers rule that install.sh created. Leaves server/ as it is,
# including generated fixtures, the venv, the Rust toolchain and the binaries.
# usage: sudo deploy/uninstall.sh
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "Run as root, with sudo" >&2; exit 1; }
systemctl stop synthetic-rest-toxiproxy synthetic-rest-api 2> /dev/null || true
rm -f /etc/systemd/system/synthetic-rest-api.service /etc/systemd/system/synthetic-rest-toxiproxy.service \
  /etc/sudoers.d/synthetic-rest
systemctl daemon-reload
echo "Removed the synthetic API services."
