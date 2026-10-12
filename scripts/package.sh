#!/usr/bin/env bash
#
# 打包一个 aria 发布产物: dist/aria-<标签>-<目标三元组>.tar.gz 与同名 .sha256 文件.
#
# 产物名称与压缩包内容是下载方按名称取用这两个文件的契约: 解压后直接得到名为 aria 的可执行
# 文件, 因此压缩包内不允许有其他成员, 可执行文件也不能带目录前缀. .sha256 文件采用 sha256sum
# 的输出格式, 即哈希, 两个空格, 文件名, 使 sha256sum -c 与 shasum -a 256 -c 都能在 dist 目录
# 内直接校验.
#
# Packages one aria release artifact: dist/aria-<tag>-<target>.tar.gz with its .sha256 file.
# The artifact name and archive layout are the contract for downloaders that fetch both files by
# name and expect exactly one executable named aria at the archive root. The checksum file uses the
# sha256sum output format so both sha256sum -c and shasum -a 256 -c verify it directly from inside
# dist.
#
# 用法: scripts/package.sh <目标三元组> <标签> <可执行文件路径>
# Usage: scripts/package.sh <target-triple> <tag> <binary-path>

set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
用法: scripts/package.sh <目标三元组> <标签> <可执行文件路径>

  目标三元组   Rust 目标名, 例如 aarch64-apple-darwin
  标签         发布标签, 原样出现在产物名称中, 例如 v0.1.0
  可执行文件   cargo 构建出的 aria 可执行文件路径

输出:
  dist/aria-<标签>-<目标三元组>.tar.gz
  dist/aria-<标签>-<目标三元组>.tar.gz.sha256
USAGE
}

# 参数缺失或多出时只打印用法, 不猜测调用者意图; 2 是命令行用法错误的约定退出码.
if [[ $# -ne 3 ]]; then
  usage
  exit 2
fi

target=$1
tag=$2
binary=$3

if [[ ! -f "$binary" ]]; then
  echo "错误: 可执行文件不存在: $binary" >&2
  exit 1
fi

# 输出固定放在仓库根目录的 dist 下, 使脚本在任意工作目录调用都得到同一结果.
root=$(cd "$(dirname "$0")/.." && pwd)
dist=$root/dist
archive="aria-${tag}-${target}.tar.gz"

mkdir -p "$dist"

# 先在临时目录中把可执行文件按最终名称放好, 再把该目录作为归档根目录, 因此归档内只有
# aria 一个成员且没有路径前缀.
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
install -m 755 "$binary" "$stage/aria"

# macOS 的 bsdtar 会把扩展属性写成 AppleDouble 伴随成员, COPYFILE_DISABLE 关闭这一行为,
# 保证压缩包内只有 aria 一个成员; GNU tar 忽略该变量.
COPYFILE_DISABLE=1 tar -czf "$dist/$archive" -C "$stage" aria

# 校验文件记录裸文件名, 这样在 dist 目录内执行校验命令时不需要改写路径.
# Linux 使用 sha256sum, macOS 没有该命令时退回 shasum -a 256, 两者输出格式相同.
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$dist" && sha256sum "$archive" > "$archive.sha256")
else
  (cd "$dist" && shasum -a 256 "$archive" > "$archive.sha256")
fi

echo "已生成 $dist/$archive 与 $dist/$archive.sha256"
