#!/usr/bin/env bash
# Run explicitly on a dedicated Ubuntu VM after joining the team's Tailscale network.
set -euo pipefail
mode="${1:-}"
node_name="${2:-}"
server_ip="${3:-}"
if [[ "$mode" != server && "$mode" != agent ]] || [[ ! "$node_name" =~ ^[a-z0-9][a-z0-9-]*$ ]] || [[ ${#node_name} -gt 63 ]]; then
  echo "Usage: sudo bash infra/k3s/install-node.sh server steam-server"
  echo "       sudo bash infra/k3s/install-node.sh agent steam-worker-1 SERVER_TAILSCALE_IP"
  exit 2
fi
[[ "$EUID" == 0 ]] || { echo "Run using sudo on the target VM."; exit 1; }
for tool in tailscale curl systemctl ip; do command -v "$tool" >/dev/null; done
[[ -d /run/systemd/system ]] || { echo "A VM with systemd is required."; exit 1; }
[[ ! -e /etc/rancher/k3s/config.yaml ]] || { echo "Existing k3s config found; refusing to replace it."; exit 1; }
if command -v k3s >/dev/null; then
  echo "k3s already installed; refusing to reinstall or upgrade automatically."; exit 1
fi
ip link show tailscale0 >/dev/null
node_ip="$(tailscale ip -4)"
[[ "$node_ip" =~ ^100\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Join Tailscale before installing k3s."; exit 1; }
if [[ "$mode" == agent ]]; then
  [[ "$server_ip" =~ ^100\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Supply the server Tailscale IPv4 address."; exit 2; }
  tailscale ping --c 3 "$server_ip" >/dev/null
fi
umask 077
mkdir -p /etc/rancher/k3s
config=/etc/rancher/k3s/config.yaml
cat > "$config" <<EOF
node-name: "$node_name"
node-ip: "$node_ip"
flannel-iface: tailscale0
EOF
if [[ "$mode" == server ]]; then
  cat >> "$config" <<EOF
advertise-address: "$node_ip"
tls-san:
  - "$node_ip"
flannel-backend: vxlan
disable:
  - traefik
  - servicelb
write-kubeconfig-mode: "0600"
EOF
else
  read -r -s -p "Paste the server node-token (input hidden): " join_token
  echo
  [[ -n "$join_token" ]] || { echo "Empty token."; exit 1; }
  printf '%s\n' "$join_token" > /etc/rancher/k3s/join-token
  unset join_token
  cat >> "$config" <<EOF
server: "https://$server_ip:6443"
token-file: /etc/rancher/k3s/join-token
EOF
fi
installer="$(mktemp)"
trap 'rm -f "$installer"' EXIT
curl -fsSL https://get.k3s.io -o "$installer"
INSTALL_K3S_VERSION=v1.34.12+k3s1 INSTALL_K3S_EXEC="$mode" sh "$installer"
service_name=k3s
if [[ "$mode" == agent ]]; then service_name=k3s-agent; fi
echo "Installed $mode as $node_name. Check: sudo systemctl status $service_name"
if [[ "$mode" == server ]]; then
  echo "Use sudo k3s kubectl get nodes -o wide to inspect the cluster."
  echo "Join token is in /var/lib/rancher/k3s/server/node-token; share privately with worker owners."
fi
