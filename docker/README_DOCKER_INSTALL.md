# Rootless Docker on Debian — README

A concise, copy‑paste friendly guide to install and run **Docker Engine in rootless mode** on **Debian 11/12/13** using only the command line.

> Rootless mode runs the Docker daemon and containers **without root privileges**, improving isolation on multi‑user systems. The setup commands below avoid unnecessary elevation; only package installation and repo setup require `sudo`.

---

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [If Your Account Is Root (UID 0)](#if-your-account-is-root-uid-0)
- [Step-by-Step (Explained)](#step-by-step-explained)
- [Post‑Install: Environment, Autostart & Compose](#post-install-environment-autostart--compose)
- [Verification](#verification)
- [Common Issues & Fixes](#common-issues--fixes)
- [Uninstall / Revert to Rootful](#uninstall--revert-to-rootful)
- [Cheat Sheet](#cheat-sheet)

---

## Overview

- **Do not run the rootless installer as root.** Use a real non‑root account (UID ≠ 0).
- The rootless daemon uses a user‑scoped socket at: `unix:///run/user/<uid>/docker.sock`.
- Rootless requires user namespaces and helpers: `uidmap`, `slirp4netns`, `fuse-overlayfs`, and systemd **user** services.

---

## Quick Start

> Run **steps 1–3 with `sudo`**. From **step 5 onward, run as your normal user** (no sudo).

```bash
# 1) Prereqs
sudo apt-get update
sudo apt-get install -y ca-certificates curl uidmap dbus-user-session slirp4netns fuse-overlayfs

# 2) Add Docker’s official APT repo
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

# 3) Install Docker Engine + rootless extras
sudo apt-get install -y docker-ce docker-ce-cli containerd.io   docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras

# 4) (Optional) Stop the rootful daemon if it’s running
sudo systemctl disable --now docker.service docker.socket || true
sudo rm -f /var/run/docker.sock

# 5) Initialize rootless Docker (run as your normal user, no sudo)
dockerd-rootless-setuptool.sh install

# 6) Autostart & manage the user service
sudo loginctl enable-linger "$USER"
systemctl --user enable --now docker

# 7) Use Docker (rootless)
echo 'export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock' >> ~/.bashrc
export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock
docker run --rm hello-world
```

---

## If Your Account Is Root (UID 0)

Rootless Docker refuses to install for UID 0. Create a real non‑root user and finish setup there.

```bash
# As root:
adduser dockeruser                 # follow prompts
usermod -aG sudo dockeruser        # optional: grant sudo
usermod --add-subuids 100000-165536 --add-subgids 100000-165536 dockeruser
loginctl enable-linger dockeruser  # optional: allow user services at boot

# Stop rootful docker to avoid confusion:
systemctl disable --now docker.service docker.socket || true
rm -f /var/run/docker.sock

# Switch to the new user:
su - dockeruser

# As the non-root user:
dockerd-rootless-setuptool.sh install
systemctl --user enable --now docker
echo 'export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock' >> ~/.bashrc
export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock
docker run --rm hello-world
```

> Quick checks: `id -u` should be **non‑zero**. If you see “XDG_RUNTIME_DIR not set”, export it as shown in *Common Issues* below.

---

## Step-by-Step (Explained)

### 1) Install prerequisites
```bash
sudo apt-get update
sudo apt-get install -y uidmap dbus-user-session slirp4netns fuse-overlayfs ca-certificates curl
```
- `uidmap`: provides `newuidmap`/`newgidmap` for user namespaces  
- `dbus-user-session`: enables `systemd --user` services  
- `slirp4netns`: user‑mode networking for containers  
- `fuse-overlayfs`: layered filesystem usable without root

### 2) Add Docker’s official APT repository
```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```
Keeps Engine & tooling current on Debian 11/12/13.

### 3) Install Docker Engine and rootless extras
```bash
sudo apt-get install -y docker-ce docker-ce-cli containerd.io   docker-buildx-plugin docker-compose-plugin docker-ce-rootless-extras
```
`docker-ce-rootless-extras` provides the rootless setup tool and helpers.

### 4) (Optional) Disable any rootful Docker daemon
```bash
sudo systemctl disable --now docker.service docker.socket || true
sudo rm -f /var/run/docker.sock
```
Avoids confusion between rootful and rootless daemons.

### 5) Initialize rootless Docker (as non‑root)
```bash
dockerd-rootless-setuptool.sh install
```
Creates user config and a `systemd --user` service for `dockerd`.

### 6) Enable autostart and start the daemon
```bash
sudo loginctl enable-linger "$USER"   # start on boot even if you don't log in
systemctl --user enable --now docker
```

### 7) Point the CLI at your user socket
```bash
echo 'export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock' >> ~/.bashrc
export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock
```

---

## Post‑Install: Environment, Autostart & Compose

- **Persist CLI config**: the `DOCKER_HOST` line in `~/.bashrc` ensures new shells talk to the rootless daemon.
- **Compose**: `docker compose` works normally; no extra flags required.
- **Data root**: rootless Docker stores data at `~/.local/share/docker` (per‑user).
- **Cgroups**: For `--memory`, `--cpus`, etc., ensure **cgroup v2** (Debian 12 defaults to it). Check in [Verification](#verification).

---

## Verification

```bash
# Check that Docker is rootless
docker info | grep -iE 'rootless|cgroup version'

# Expect to see:
#   Security Options:
#    rootless
#   Cgroup Version: 2

# Run a test container
docker run --rm hello-world
```

---

## Common Issues & Fixes

- **“Refusing to install rootless Docker as the root user”**  
  Your account is UID 0. Create and use a non‑root user (see section above).

- **`systemd --user` not detected / D-Bus issues**  
  Log in as the target user with a *real* login shell (`su - username` or SSH) and rerun the setup. Then use `systemctl --user …` commands.

- **`XDG_RUNTIME_DIR` unset**  
  ```bash
  export XDG_RUNTIME_DIR="/run/user/$(id -u)"
  mkdir -p "$XDG_RUNTIME_DIR"
  chmod 700 "$XDG_RUNTIME_DIR"
  ```

- **Ports < 1024 (80/443) won’t bind**  
  Rootless daemons can’t bind privileged ports directly. Use higher host ports (e.g., `-p 8080:80`) or a rootful reverse proxy that forwards to your rootless service.

- **Resource limits don’t apply**  
  Ensure cgroup v2 is active:
  ```bash
  [ -f /sys/fs/cgroup/cgroup.controllers ] && echo "cgroup v2 detected"
  ```

---

## Uninstall / Revert to Rootful

```bash
# Disable the rootless user service (as the same non-root user)
systemctl --user disable --now docker

# (Optional) Remove user data
docker system prune -a
rm -rf ~/.local/share/docker ~/.config/docker

# Re-enable and start rootful daemon (as root)
sudo systemctl enable --now docker
```

---

## Cheat Sheet

```bash
# Manage the rootless daemon
systemctl --user (start|stop|restart|status) docker

# Where the socket lives
echo $DOCKER_HOST
# unix:///run/user/$(id -u)/docker.sock

# One-liner to set the socket for your current shell
export DOCKER_HOST=unix:///run/user/$(id -u)/docker.sock
```
