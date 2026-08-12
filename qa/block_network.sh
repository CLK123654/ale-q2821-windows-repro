#!/usr/bin/env bash
set -euo pipefail
repo_root="$1"
sudo ip link set eth0 down
ip -j link show > "$repo_root/evidence/network-guard.json"
