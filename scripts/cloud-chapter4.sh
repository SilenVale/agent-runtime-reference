#!/bin/bash
# 第 4 章云端：对真实 PostgreSQL + MySQL 假写逐条取证。4.3 依赖改存储后的 demo 证据，本脚本不重跑三段演示。
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
runtime_dir="$root/.runtime"
compose_file="$root/deploy/demo/docker-compose.yml"
evidence="$root/docs/demo/chapter4-云端.txt"
mkdir -p "$runtime_dir/lib" "$runtime_dir/pids" "$root/docs/demo" "$root/out"

export PGHOST="${PGHOST:-127.0.0.1}" PGPORT="${PGPORT:-5432}" PGUSER="${PGUSER:-runtime}"
export PGPASSWORD="${PGPASSWORD:-runtime}" PGDATABASE="${PGDATABASE:-runtime}"
export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}" MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_USER="${MYSQL_USER:-fake}" MYSQL_PASSWORD="${MYSQL_PASSWORD:-fake}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-fake_write}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-runtime.schedule}"
export KAFKA_GROUP="runtime-workers-ch4-$$"
export ACCESS_PORT="${ACCESS_PORT:-8080}" LEASE_MS="${DEMO_LEASE_MS:-5000}"

fail() { echo "第4章云端未通过：$1" >&2; exit 1; }
docker_bin() {
  if docker info >/dev/null 2>&1; then echo docker
  elif sudo docker info >/dev/null 2>&1; then echo "sudo docker"
  else fail "没有 Docker"; fi
}
DOCKER=$(docker_bin)
compose() { $DOCKER compose -f "$compose_file" "$@"; }
java_run() { java -cp "out:$runtime_dir/lib/*" "$@"; }

compose up -d
for _ in $(seq 1 90); do
  ok=0
  compose exec -T postgres pg_isready -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1 || continue
  compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -proot --silent >/dev/null 2>&1 || continue
  ok=1; break
done
[[ "$ok" -eq 1 ]] || fail "PG/MySQL 未就绪"

base=https://repo1.maven.org/maven2
[[ -f "$runtime_dir/lib/postgresql-42.7.4.jar" ]] || curl -fsSL -o "$runtime_dir/lib/postgresql-42.7.4.jar" "$base/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
[[ -f "$runtime_dir/lib/mysql-connector-j-8.4.0.jar" ]] || curl -fsSL -o "$runtime_dir/lib/mysql-connector-j-8.4.0.jar" "$base/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar"

find "$root/底座" "$root/假业务" -name '*.java' | sort > "$root/out/sources.txt"
javac --release 21 -encoding UTF-8 -cp "$runtime_dir/lib/*" -d "$root/out" @"$root/out/sources.txt"
java_run live.EnsureSchema || fail "schema"

: > "$evidence"
{
  echo "服务=postgres:16.15:5432 mysql:8.4:3306/$MYSQL_DATABASE"
  echo "## 4.1"
  java_run demo.Chapter4Cloud reset >/dev/null
  java_run demo.Chapter4Cloud r1
} | tee -a "$evidence"
grep -q '旧代次提交 changed=0' "$evidence" || fail "4.1 旧代次"
grep -q '数据库时钟未到期续租 changed=1' "$evidence" || fail "4.1 续租"
grep -q '数据库时钟已到期续租 changed=0' "$evidence" || fail "4.1 到期"

{
  echo "## 4.2"
  java_run demo.Chapter4Cloud reset >/dev/null
  java_run demo.Chapter4Cloud r2
} | tee -a "$evidence"
grep -q 'same=true' "$evidence" || fail "4.2 同号"
grep -q '账本 status=PLANNED' "$evidence" || fail "4.2 账本"
grep -q '换参数拒绝=true' "$evidence" || fail "4.2 换参数"

# 4.3 不在此重跑，见改存储后的 docs/demo/三段演示.txt

{
  echo "## 4.4"
  java_run demo.Chapter4Cloud r4
} | tee -a "$evidence"
grep -q '账本已提交 ops=1 Checkpoint回滚 cps=0' "$evidence" || fail "4.4 账本"
grep -q '图位置单独提交 cps=1 未提交账本 ops=0' "$evidence" || fail "4.4 图位置"

{
  echo "## 4.5"
  java_run demo.Chapter4Cloud reset >/dev/null
  java_run demo.Chapter4Cloud r5
} | tee -a "$evidence"
grep -q '接管后 spent=40 epoch=2' "$evidence" || fail "4.5 累计"
grep -q '旧代次清零 changed=0' "$evidence" || fail "4.5 旧代次"
grep -q '新预留后 spent=40 reserved=10' "$evidence" || fail "4.5 预留"

{
  echo "## 4.6"
  java_run demo.Chapter4Cloud reset >/dev/null
  java_run demo.Chapter4Cloud r6
} | tee -a "$evidence"
grep -q '重复预留后 reserved=10' "$evidence" || fail "4.6 预留"
grep -q '重复结算后 spent=47' "$evidence" || fail "4.6 结算"
grep -q 'reserved=0 settled=7' "$evidence" || fail "4.6 回执"

{
  echo "## 4.7"
  java_run demo.Chapter4Cloud reset >/dev/null
  java_run demo.Chapter4Cloud r7
} | tee -a "$evidence"
grep -q '退出后预留 state=RESERVED reserved=12 spent=40' "$evidence" || fail "4.7"

{
  echo "## 4.8"
  java_run demo.Chapter4Cloud reset >/dev/null
  java_run demo.Chapter4Cloud r8
} | tee -a "$evidence"
grep -q '旧代次登记 changed=0' "$evidence" || fail "4.8 旧代次"
grep -q '当前引用 recovery_ref=' "$evidence" || fail "4.8 引用"
grep -q 'epoch-1' "$evidence" || fail "4.8 引用内容"

echo "第4章云端规则（除4.3用demo证据）跑完：$evidence"
