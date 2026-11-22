#!/bin/bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
for chapter in 1 2 3 4 5 6 7 8 9 10 11 12 13 14; do
  echo "== 第 ${chapter} 章 =="
  scripts/run-chapter.sh "$chapter"
done
echo "== 三段演示 =="
scripts/demo.sh
