#!/usr/bin/env bash
set -euo pipefail

backup_if_missing() {
    local path="$1"
    local backup="${path}.codex.bak"
    if [[ -e "$path" && ! -e "$backup" ]]; then
        cp -a "$path" "$backup"
    fi
}

backup_if_missing /etc/wsl.conf
backup_if_missing /etc/resolv.conf

cat > /etc/wsl.conf <<'EOF'
[boot]
systemd=true

[user]
default=seok436

[network]
generateResolvConf=false
EOF

rm -f /etc/resolv.conf
cat > /etc/resolv.conf <<'EOF'
nameserver 1.1.1.1
nameserver 8.8.8.8
EOF

chmod 644 /etc/wsl.conf /etc/resolv.conf
echo "WSL DNS config updated. Run 'wsl --shutdown' from Windows to apply."
