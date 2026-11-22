#!/bin/bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

runtime_dir="$root/.runtime"
compose_file="$root/deploy/demo/docker-compose.yml"
mkdir -p "$runtime_dir/lib" "$runtime_dir/pids" "$root/docs/demo" "$root/out"

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-runtime}"
export PGPASSWORD="${PGPASSWORD:-runtime}"
export PGDATABASE="${PGDATABASE:-runtime}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-runtime.schedule}"
export KAFKA_GROUP="runtime-workers-demo-$$"
export ACCESS_PORT="${ACCESS_PORT:-8080}"
# 假写在 MySQL，与任务库（PostgreSQL）分开提交。
export MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_USER="${MYSQL_USER:-fake}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-fake}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-fake_write}"
# 租约短一点，方便演示里等它自己到期。禁止脚本改 lease_expires_at。
# 只用 DEMO_LEASE_MS，避免外壳里残留的 LEASE_MS 把等待拉长。
export LEASE_MS="${DEMO_LEASE_MS:-5000}"

fail() {
  echo "演示未通过：$1" >&2
  exit 1
}

docker_bin() {
  if docker info >/dev/null 2>&1; then
    echo docker
  elif sudo docker info >/dev/null 2>&1; then
    echo "sudo docker"
  else
    fail "本机没有可用的 Docker。演示只靠 Docker 拉起 PostgreSQL 和 Kafka，不能改用本机安装或进程内模型。"
  fi
}

DOCKER=$(docker_bin)
compose() {
  $DOCKER compose -f "$compose_file" "$@"
}

psql_runtime() {
  compose exec -T postgres \
    env PGPASSWORD="$PGPASSWORD" \
    psql -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 "$@"
}

ensure_docker_services() {
  command -v java >/dev/null 2>&1 || fail "本机没有 java。三个进程要用本机 JDK 跑，服务才只用 Docker。"
  command -v javac >/dev/null 2>&1 || fail "本机没有 javac。"
  compose up -d
  echo "等待 PostgreSQL、Kafka、MySQL（Docker）就绪…"
  ready_pg=0
  ready_kafka=0
  ready_mysql=0
  for _ in $(seq 1 90); do
    if compose exec -T postgres pg_isready -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; then
      ready_pg=1
    fi
    if compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1; then
      ready_kafka=1
    fi
    if compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -proot --silent >/dev/null 2>&1; then
      ready_mysql=1
    fi
    if [[ "$ready_pg" -eq 1 && "$ready_kafka" -eq 1 && "$ready_mysql" -eq 1 ]]; then
      break
    fi
    sleep 1
  done
  [[ "$ready_pg" -eq 1 ]] || fail "Docker 里的 PostgreSQL 没起来。"
  [[ "$ready_kafka" -eq 1 ]] || fail "Docker 里的 Kafka 没起来。"
  [[ "$ready_mysql" -eq 1 ]] || fail "Docker 里的 MySQL 没起来。"
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
    --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1 >/dev/null
  echo "PostgreSQL 已由 Docker 提供 $PGHOST:$PGPORT/$PGDATABASE"
  echo "Kafka 已由 Docker 提供 $KAFKA_BOOTSTRAP topic=$KAFKA_TOPIC"
  echo "MySQL 假写已由 Docker 提供 $MYSQL_HOST:$MYSQL_PORT/$MYSQL_DATABASE"
}

ensure_jars() {
  local base=https://repo1.maven.org/maven2
  download() {
    local url="$1" dest="$2"
    if [[ ! -f "$dest" ]]; then
      curl -fsSL -o "$dest" "$url" || fail "下不了 $dest"
    fi
  }
  download "$base/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" "$runtime_dir/lib/postgresql-42.7.4.jar"
  download "$base/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar" "$runtime_dir/lib/mysql-connector-j-8.4.0.jar"
  download "$base/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar" "$runtime_dir/lib/kafka-clients-3.9.0.jar"
  download "$base/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar" "$runtime_dir/lib/slf4j-api-1.7.36.jar"
  download "$base/org/slf4j/slf4j-reload4j/1.7.36/slf4j-reload4j-1.7.36.jar" "$runtime_dir/lib/slf4j-reload4j-1.7.36.jar"
  download "$base/ch/qos/reload4j/reload4j/1.2.25/reload4j-1.2.25.jar" "$runtime_dir/lib/reload4j-1.2.25.jar"
  download "$base/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" "$runtime_dir/lib/lz4-java-1.8.0.jar"
  download "$base/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" "$runtime_dir/lib/snappy-java-1.1.10.5.jar"
  download "$base/com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar" "$runtime_dir/lib/zstd-jni-1.5.6-4.jar"
}

compile_all() {
  find "$root/底座" "$root/假业务" -name '*.java' | sort > "$root/out/sources.txt"
  javac --release 21 -encoding UTF-8 -cp "$runtime_dir/lib/*" -d "$root/out" @"$root/out/sources.txt"
}

start_process() {
  local name="$1"
  shift
  local pidfile="$runtime_dir/pids/$name.pid"
  local logfile="$runtime_dir/pids/$name.log"
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    sleep 0.5
  fi
  (
    cd "$root"
    export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE
    export MYSQL_HOST MYSQL_PORT MYSQL_USER MYSQL_PASSWORD MYSQL_DATABASE
    export KAFKA_BOOTSTRAP KAFKA_TOPIC KAFKA_GROUP ACCESS_PORT LEASE_MS
    exec java -cp "out:$runtime_dir/lib/*" "$@"
  ) >"$logfile" 2>&1 &
  echo $! >"$pidfile"
}

stop_process() {
  local name="$1"
  local pidfile="$runtime_dir/pids/$name.pid"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.2
      done
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile"
  fi
}

cleanup() {
  stop_process access || true
  stop_process dispatch || true
  stop_process runtime-a || true
  stop_process runtime-b || true
}
trap cleanup EXIT

ensure_docker_services
ensure_jars
compile_all

java -cp "out:$runtime_dir/lib/*" live.EnsureSchema \
  || fail "PostgreSQL schema 建不起来。"

pg_version=$(psql_runtime -Atc 'SELECT version()')
# pipefail 下 head 会提前关管道，这里允许描述命令非零退出。
kafka_desc=$(compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --describe --topic "$KAFKA_TOPIC" 2>/dev/null | head -1 | tr -d '\r' || true)
[[ -n "$kafka_desc" ]] || kafka_desc="topic=$KAFKA_TOPIC (Docker apache/kafka:3.9.0)"
mysql_version=$(compose exec -T mysql mysql -N -uroot -proot -e 'SELECT VERSION();' 2>/dev/null | tr -d '\r')
{
  echo "PostgreSQL=$pg_version"
  echo "Kafka=$kafka_desc"
  echo "MySQL假写=$mysql_version db=$MYSQL_DATABASE"
  echo "服务启动方式=Docker Compose ($compose_file)"
  echo "消费组=$KAFKA_GROUP"
  echo "租约等待=自然到期（脚本不改 lease_expires_at）"
  echo "进程内Database=未使用"
  echo "进程内KafkaLog=未使用"
} > "$root/docs/demo/环境.txt"

start_process access live.LiveAccessServer
start_process dispatch live.LiveDispatchLoop
start_process runtime-a live.LiveWorkerLoop pod-a

for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:$ACCESS_PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -fsS "http://127.0.0.1:$ACCESS_PORT/health" >/dev/null || fail "接入层没有起来。日志：$runtime_dir/pids/access.log"

java -cp "out:$runtime_dir/lib/*" demo.LiveDemoDriver accept | tee "$root/docs/demo/三段演示.txt"
task_id=$(awk '/^受理 /{print $3; exit}' "$root/docs/demo/三段演示.txt")
[[ -n "$task_id" ]] || fail "没有拿到 task_id"

ready=0
for _ in $(seq 1 100); do
  owner=$(psql_runtime -Atc "SELECT owner FROM tasks WHERE task_id='$task_id' AND epoch=1 AND owner IS NOT NULL")
  if [[ -n "$owner" ]]; then
    ready=1
    break
  fi
  sleep 0.2
done
[[ "$ready" -eq 1 ]] || fail "pod-a 没有接管。日志：$runtime_dir/pids/runtime-a.log"
{
  echo "接管 epoch=1 owner=$owner"
} | tee -a "$root/docs/demo/三段演示.txt"

java -cp "out:$runtime_dir/lib/*" demo.LiveDemoDriver reject "$task_id" "$owner" | tee -a "$root/docs/demo/三段演示.txt"

# 杀掉第一个 Worker，只等租约自己到期，不改 lease_expires_at。
stop_process runtime-a
echo "已杀掉 runtime-a，等待租约自然到期（LEASE_MS=${LEASE_MS}）…" | tee -a "$root/docs/demo/三段演示.txt"

expired=0
for _ in $(seq 1 120); do
  row=$(psql_runtime -Atc "SELECT CASE WHEN lease_expires_at <= now() THEN 'expired' ELSE 'live' END FROM tasks WHERE task_id='$task_id'")
  if [[ "$row" == "expired" ]]; then
    expired=1
    break
  fi
  sleep 0.25
done
[[ "$expired" -eq 1 ]] || fail "等待租约自然到期超时。脚本没有改 lease_expires_at。"
echo "租约已自然到期" | tee -a "$root/docs/demo/三段演示.txt"

start_process runtime-b live.LiveWorkerLoop pod-b

ready=0
for _ in $(seq 1 200); do
  next=$(psql_runtime -Atc \
    "SELECT owner FROM tasks WHERE task_id='$task_id' AND epoch=2 AND owner IS NOT NULL AND owner<>'$owner'")
  if [[ -n "$next" ]]; then
    ready=1
    break
  fi
  sleep 0.25
done
[[ "$ready" -eq 1 ]] || fail "pod-b 没有在租约自然到期后把 epoch 接到 2。日志：$runtime_dir/pids/runtime-b.log"
{
  echo "另一个Worker继续 epoch=2 owner=$next"
} | tee -a "$root/docs/demo/三段演示.txt"

java -cp "out:$runtime_dir/lib/*" demo.LiveDemoDriver fakewrite "$task_id" | tee -a "$root/docs/demo/三段演示.txt"

# 三条硬条件
grep -q '任务数=1' "$root/docs/demo/三段演示.txt" || fail "同幂等键任务数不是 1"
grep -q '另一个Worker继续 epoch=2' "$root/docs/demo/三段演示.txt" || fail "后一个进程没有把 epoch 接到 2"
grep -q '写入次数=1' "$root/docs/demo/三段演示.txt" || fail "同一个 operation_id 写入次数不是 1"

grep -q '代次拒绝' "$root/docs/demo/三段演示.txt" || fail "没有看到代次拒绝"
grep -q '服务启动方式=Docker Compose' "$root/docs/demo/环境.txt" || fail "环境记录没有标明 Docker Compose"
grep -q '租约等待=自然到期' "$root/docs/demo/环境.txt" || fail "环境记录没有标明自然到期"
grep -q '进程内Database=未使用' "$root/docs/demo/环境.txt" || fail "环境记录缺失"

echo "演示通过：Docker 拉起 PostgreSQL/Kafka，杀掉第一个 Worker 后只等租约到期，三条都成立。"
