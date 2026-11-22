#!/bin/bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/out"
mkdir -p "$out" "$root/docs/demo"
find "$root/底座" "$root/假业务" -name '*.java' | sort > "$out/sources.txt"
javac --release 21 -encoding UTF-8 -d "$out" @"$out/sources.txt"
cd "$root"
java -cp "$out" demo.ThreeIncidents | tee "$root/docs/demo/三段演示.txt"
