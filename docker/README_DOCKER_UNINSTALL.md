# Uninstall Docker on Debian/Ubuntu

> This guide **completely removes** Docker Engine / Docker Desktop components installed via APT or Snap on Debian and Ubuntu systems. It also wipes **images, containers, volumes, and configs** if you choose the cleanup steps. Proceed carefully and back up any data you care about (especially named volumes).

---

## Supported
- Debian 10+ (buster, bullseye, bookworm)
- Ubuntu 18.04+ (bionic, focal, jammy, noble)

> Commands are intended for **systemd**-based installs run with `sudo` privileges.

---

## TL;DR (nuke everything)
> Irreversible cleanup of containers, images, volumes, packages, repos, and data.

```bash
# 1) Stop services
sudo systemctl disable --now docker docker.socket containerd 2>/dev/null || true

# 2) Remove all containers, images, volumes, and build cache (optional but recommended before purge)
docker ps -q | xargs -r docker stop
docker ps -aq | xargs -r docker rm -vf
docker image prune -a -f
docker builder prune -a -f
docker volume ls -q | xargs -r docker volume rm
docker network ls -q | grep -vE '(^| )(host|none|bridge)($| )' | xargs -r docker network rm

# 3) Purge Debian/Ubuntu packages (works for both upstream Docker CE and distro docker.io)
sudo apt-get purge -y docker-ce docker-ce-cli docker.io containerd.io docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras docker-scan-plugin

# 4) Remove Snap, if it was used
command -v snap >/dev/null 2>&1 && sudo snap remove docker 2>/dev/null || true

# 5) Remove Docker APT repo & key if present, then refresh
sudo rm -f /etc/apt/sources.list.d/docker*.list
sudo rm -f /etc/apt/keyrings/docker.gpg
sudo apt-get autoremove -y && sudo apt-get update

# 6) Delete all Docker data & configs (engine + containerd)
sudo rm -rf /var/lib/docker /var/lib/containerd /etc/docker
sudo rm -f  /var/run/docker.sock

# 7) Remove per-user config & legacy Compose
rm -rf ~/.docker ~/.local/share/docker
sudo rm -f /usr/local/bin/docker-compose

# 8) Optional: remove bridge interface & group
sudo ip link delete docker0 2>/dev/null || true
sudo groupdel docker 2>/dev/null || true

# 9) Verify removal
command -v docker >/dev/null || echo "docker not found"
systemctl status docker 2>/dev/null || echo "docker service not found"
```

---

## Step-by-step (safer, with explanations)

### 1) Identify how Docker was installed
List installed packages:
```bash
dpkg -l | grep -E '^ii\s+(docker|containerd|runc)'
```
Common packages to remove:
- `docker-ce`, `docker-ce-cli`, `docker-ce-rootless-extras`, `docker-buildx-plugin`, `docker-compose-plugin`
- `docker.io` (distro package)
- `containerd.io` (upstream containerd bundled with Docker)
- `docker-scan-plugin` (legacy)

If Docker came from **Snap**:
```bash
snap list | grep -i docker
```

### 2) Stop services
```bash
sudo systemctl disable --now docker docker.socket containerd 2>/dev/null || true
```

### 3) (Optional but recommended) Clean up runtime objects
Stop and remove containers, images, volumes, networks, and build cache:
```bash
docker ps -q | xargs -r docker stop
docker ps -aq | xargs -r docker rm -vf
docker image prune -a -f
docker builder prune -a -f
docker volume ls -q | xargs -r docker volume rm
docker network ls -q | grep -vE '(^| )(host|none|bridge)($| )' | xargs -r docker network rm
```
> **Skip** if you plan to keep your images/volumes for a re-install.

### 4) Purge APT packages (Debian/Ubuntu)
This covers both upstream Docker CE and the `docker.io` package:
```bash
sudo apt-get purge -y docker-ce docker-ce-cli docker.io containerd.io docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras docker-scan-plugin
sudo apt-get autoremove -y
```

### 5) Remove Docker APT repo & key (if present)
```bash
sudo rm -f /etc/apt/sources.list.d/docker*.list
sudo rm -f /etc/apt/keyrings/docker.gpg
sudo apt-get update
```

### 6) Remove Snap install (if used)
```bash
command -v snap >/dev/null 2>&1 && sudo snap remove docker || true
```

### 7) Delete data and configuration
Engine & containerd data:
```bash
sudo rm -rf /var/lib/docker /var/lib/containerd
```
System configs & socket:
```bash
sudo rm -rf /etc/docker
sudo rm -f /var/run/docker.sock
```
Per-user configs and **rootless** data:
```bash
rm -rf ~/.docker ~/.local/share/docker
```
Legacy Compose v1 binary (if you ever installed it to /usr/local):
```bash
sudo rm -f /usr/local/bin/docker-compose
```

### 8) Optional network & group cleanup
```bash
sudo ip link delete docker0 2>/dev/null || true
sudo groupdel docker 2>/dev/null || true
```

### 9) Verify
```bash
command -v docker || echo "docker not found"
systemctl status docker 2>/dev/null || echo "docker service not found"
```

---

## Notes & Edge Cases

- **containerd**: If you use containerd separately (e.g., with Kubernetes), do **not** purge `containerd.io`. Adjust the purge list accordingly.
- **Rootless Docker**: Data lives under `~/.local/share/docker` and config under `~/.config/docker` / `~/.docker`. The commands above remove these.
- **Pip/Pipx Docker Compose**: If you installed Compose via Python:
  ```bash
  pipx uninstall docker-compose 2>/dev/null || true
  pip uninstall -y docker-compose 2>/dev/null || true
  ```
- **Reinstalling later**: Reboot is not required, but if you removed the `docker0` interface and see stale routes, a reboot or `systemctl daemon-reload && systemctl restart NetworkManager` (on Ubuntu Desktop) can help.

---

## Troubleshooting

- **`device or resource busy` errors when removing `/var/lib/docker`**  
  Ensure **all** containers are stopped/removed and no other services (e.g., Kubernetes, Podman) are using containerd or overlay mounts. Then retry the `rm -rf`.
  
- **APT says packages are not installed**  
  Your system might have used the distro package (`docker.io`) or upstream (`docker-ce`). It's safe to include both in the purge command.

- **`snap remove docker` fails**  
  Make sure the Docker Desktop (if any) or docker service is not running, then retry. In stubborn cases, `sudo snap stop docker` first.

- **Leftover network interfaces**  
  If `docker0` persists, delete it with `ip link delete docker0` or reboot.

---

## Quick Reinstall (optional reference)
Follow the official docs for the latest install steps. Typically:
```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo   "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu   $(. /etc/os-release && echo "$VERSION_CODENAME") stable" |   sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
```

---

**That’s it!** Your Debian/Ubuntu system should now be free of Docker. If you hit something odd, capture the error message and logs from `journalctl -u docker` and `systemctl status docker` to investigate.
