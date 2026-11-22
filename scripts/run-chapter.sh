#!/bin/bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
  echo "用法: scripts/run-chapter.sh <章号>" >&2
  exit 2
fi
chapter="$1"
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/out"
rm -rf "$out"
mkdir -p "$out"
# 规则测试不编译 live/ 演示路径，避免强依赖本机 Kafka/JDBC jar。
find "$root/底座" "$root/假业务" -name '*.java' \
  ! -path '*/live/*' \
  ! -path '*/demo/*' \
  | sort > "$out/sources.txt"
javac --release 21 -encoding UTF-8 -d "$out" @"$out/sources.txt"
cd "$root"
java -cp "$out" "chapter${chapter}.Chapter${chapter}Tests"
