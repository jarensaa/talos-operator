#!/usr/bin/env bash
# Installs promtool, which checks and tests the chart's alert rules (make alerts-test).
#
# Prometheus cannot be installed with `go install` (its go.mod has replace directives), so this
# takes promtool out of the release archive, after checking the archive against the checksum
# written down here. To move to another release, change the version and the four checksums, from
# the release's sha256sums.txt.
set -euo pipefail

version=3.15.0
dest=${1:?usage: promtool.sh <path to install promtool at>}

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$arch" in
  x86_64) arch=amd64 ;;
  aarch64) arch=arm64 ;;
esac

case "$os-$arch" in
  linux-amd64) sum=2a542df32eac02ee17b9d844fb2aa1de00dafa5476579ba8a3ba862e9d572ea0 ;;
  linux-arm64) sum=f1f90ec08e849d494ca66c611470afc50192f0355f1a61c33f2cbde02d067823 ;;
  darwin-amd64) sum=2d79e744c2d7e505db936fbc898e05abc74fcb6e437c25befd26e9c9f00aa58b ;;
  darwin-arm64) sum=920df4d17e78b3b0175af144eb318b0c74d1cf7b1d1251b326966f0e81977260 ;;
  *) echo "promtool: no checksum for $os-$arch" >&2; exit 1 ;;
esac

if [ -x "$dest-$version" ]; then
  ln -sf "$dest-$version" "$dest"
  exit 0
fi

name=prometheus-$version.$os-$arch
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading promtool $version"
curl -fsSL --retry 3 -o "$tmp/$name.tar.gz" "https://github.com/prometheus/prometheus/releases/download/v$version/$name.tar.gz"

if command -v sha256sum >/dev/null; then
  got=$(sha256sum "$tmp/$name.tar.gz" | cut -d' ' -f1)
else
  got=$(shasum -a 256 "$tmp/$name.tar.gz" | cut -d' ' -f1)
fi

if [ "$got" != "$sum" ]; then
  echo "promtool: $name.tar.gz has checksum $got, and $sum was expected" >&2
  exit 1
fi

tar -xzf "$tmp/$name.tar.gz" -C "$tmp" "$name/promtool"
mkdir -p "$(dirname "$dest")"
mv "$tmp/$name/promtool" "$dest-$version"
ln -sf "$dest-$version" "$dest"
