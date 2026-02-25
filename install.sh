#!/usr/bin/env sh
set -eu

REPO="${IDOUD_REPO:-mydearniko/idoud}"
BIN_NAME="${IDOUD_BIN_NAME:-idoud}"
INSTALL_DIR="${IDOUD_INSTALL_DIR:-/usr/local/bin}"

log() {
  printf "%s\n" "$*"
}

fail() {
  printf "error: %s\n" "$*" >&2
  exit 1
}

detect_os() {
  case "$(uname -s)" in
    Linux) echo "linux" ;;
    Darwin) echo "darwin" ;;
    *) fail "unsupported OS: $(uname -s)" ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    i386|i686) echo "i386" ;;
    aarch64|arm64) echo "arm64" ;;
    armv5*|armv6*|armv7*|arm) echo "arm" ;;
    ppc64le|powerpc64le) echo "ppc64le" ;;
    ppc64|powerpc64) echo "ppc64" ;;
    s390x) echo "s390x" ;;
    riscv64) echo "riscv64" ;;
    mips64le) echo "mips64le" ;;
    mips64) echo "mips64" ;;
    mipsle) echo "mipsle" ;;
    mips) echo "mips" ;;
    loong64|loongarch64) echo "loong64" ;;
    *) fail "unsupported architecture: $(uname -m)" ;;
  esac
}

download() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$out"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO "$out" "$url"
    return
  fi
  fail "curl or wget is required"
}

install_binary() {
  src="$1"
  dst_dir="$2"
  dst="$dst_dir/$BIN_NAME"

  if command -v install >/dev/null 2>&1; then
    install -m 0755 "$src" "$dst"
  else
    cp "$src" "$dst"
    chmod 0755 "$dst"
  fi
}

OS="$(detect_os)"
ARCH="$(detect_arch)"

ASSET_CANDIDATES="${BIN_NAME}_${OS}_${ARCH}"
case "$ARCH" in
  amd64)
    ASSET_CANDIDATES="$ASSET_CANDIDATES ${BIN_NAME}_${OS}_amd64_v2 ${BIN_NAME}_${OS}_amd64_v1"
    ;;
  arm)
    ASSET_CANDIDATES="$ASSET_CANDIDATES ${BIN_NAME}_${OS}_arm_7 ${BIN_NAME}_${OS}_arm_6 ${BIN_NAME}_${OS}_arm_hardfloat"
    ;;
esac

TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t idoud)"
trap 'rm -rf "$TMP_DIR"' EXIT HUP INT TERM
TMP_BIN="$TMP_DIR/$BIN_NAME"

SELECTED_ASSET=""
for ASSET in $ASSET_CANDIDATES; do
  URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"
  if download "$URL" "$TMP_BIN" 2>/dev/null; then
    SELECTED_ASSET="$ASSET"
    break
  fi
done

[ -n "$SELECTED_ASSET" ] || fail "no matching release asset found for OS=${OS} ARCH=${ARCH}"
log "Downloaded ${SELECTED_ASSET} from ${REPO}."
chmod 0755 "$TMP_BIN"

if [ ! -d "$INSTALL_DIR" ]; then
  mkdir -p "$INSTALL_DIR" 2>/dev/null || true
fi

if [ -d "$INSTALL_DIR" ] && [ -w "$INSTALL_DIR" ]; then
  install_binary "$TMP_BIN" "$INSTALL_DIR"
  TARGET="$INSTALL_DIR/$BIN_NAME"
elif command -v sudo >/dev/null 2>&1; then
  sudo mkdir -p "$INSTALL_DIR"
  if command -v install >/dev/null 2>&1; then
    sudo install -m 0755 "$TMP_BIN" "$INSTALL_DIR/$BIN_NAME"
  else
    sudo cp "$TMP_BIN" "$INSTALL_DIR/$BIN_NAME"
    sudo chmod 0755 "$INSTALL_DIR/$BIN_NAME"
  fi
  TARGET="$INSTALL_DIR/$BIN_NAME"
else
  FALLBACK="$HOME/.local/bin"
  mkdir -p "$FALLBACK"
  install_binary "$TMP_BIN" "$FALLBACK"
  TARGET="$FALLBACK/$BIN_NAME"
  case ":$PATH:" in
    *":$FALLBACK:"*) ;;
    *)
      log "Add $FALLBACK to PATH to use ${BIN_NAME}:"
      log "  export PATH=\"$FALLBACK:\$PATH\""
      ;;
  esac
fi

log "Installed: $TARGET"
log "Run: ${BIN_NAME} --help"
