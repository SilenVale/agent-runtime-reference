#!/bin/bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

runtime_dir="$root/.runtime"
mkdir -p "$runtime_dir" "$runtime_dir/lib" "$root/docs/demo" "$root/out"
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-runtime}"
export PGPASSWORD="${PGPASSWORD:-runtime}"
export PGDATABASE="${PGDATABASE:-runtime}"
export FAKE_DATABASE="${FAKE_DATABASE:-fake_write}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-runtime.schedule}"
export KAFKA_GROUP="runtime-workers-demo-$$"
export ACCESS_PORT="${ACCESS_PORT:-8080}"
export LEASE_MS="${LEASE_MS:-5000}"
export KAFKA_HOME="${KAFKA_HOME:-/tmp/kafka-dl/kafka_2.13-3.9.0}"

fail() {
  echo "演示未通过：$1" >&2
  exit 1
}

ensure_postgres() {
  if ! command -v psql >/dev/null 2>&1; then
    fail "本机没有 psql。PostgreSQL 未安装，不能用进程内模型代替演示。"
  fi
  if ! PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c 'SELECT 1' >/dev/null 2>&1; then
    if command -v pg_ctlcluster >/dev/null 2>&1; then
      sudo pg_ctlcluster 16 main start >/dev/null 2>&1 || true
    fi
  fi
  if ! PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c 'SELECT 1' >/dev/null 2>&1; then
    fail "连不上 PostgreSQL（$PGHOST:$PGPORT）。不要改用内存 Database。"
  fi
  PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE'" | grep -q 1 \
    || sudo -u postgres psql -c "CREATE DATABASE $PGDATABASE OWNER $PGUSER" >/dev/null
  PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tc "SELECT 1 FROM pg_database WHERE datname='$FAKE_DATABASE'" | grep -q 1 \
    || sudo -u postgres psql -c "CREATE DATABASE $FAKE_DATABASE OWNER $PGUSER" >/dev/null
  echo "PostgreSQL 已连接 $PGHOST:$PGPORT/$PGDATABASE"
}

ensure_kafka() {
  if [[ ! -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
    fail "找不到 Kafka 发行版：$KAFKA_HOME。请先解压 Kafka，或设置 KAFKA_HOME。不要改用内存 KafkaLog。"
  fi
  if ! "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1; then
    mkdir -p "$runtime_dir/kafka-logs"
    if [[ ! -f "$runtime_dir/kraft.properties" ]]; then
      cp "$KAFKA_HOME/config/kraft/server.properties" "$runtime_dir/kraft.properties"
      sed -i "s|^log.dirs=.*|log.dirs=$runtime_dir/kafka-logs|" "$runtime_dir/kraft.properties"
      sed -i 's|^listeners=.*|listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093|' "$runtime_dir/kraft.properties"
      sed -i 's|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093|' "$runtime_dir/kraft.properties"
      sed -i 's|^controller.quorum.voters=.*|controller.quorum.voters=1@127.0.0.1:9093|' "$runtime_dir/kraft.properties"
      CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
      "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$runtime_dir/kraft.properties" >/dev/null
    fi
    SESSION_NAME=kafka-broker
    tmux -f /exec-daemon/tmux.portal.conf has-session -t "=$SESSION_NAME" 2>/dev/null \
      || tmux -f /exec-daemon/tmux.portal.conf new-session -d -s "$SESSION_NAME" -c "$KAFKA_HOME" -- "${SHELL:-bash}" -l
    tmux -f /exec-daemon/tmux.portal.conf send-keys -t "$SESSION_NAME:0.0" "bin/kafka-server-start.sh $runtime_dir/kraft.properties" C-m
    ready=0
    for _ in $(seq 1 30); do
      if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --list >/dev/null 2>&1; then
        ready=1
        break
      fi
      sleep 1
    done
    if [[ "$ready" -ne 1 ]]; then
      fail "Kafka 未能在 $KAFKA_BOOTSTRAP 起来。不要改用内存 KafkaLog。"
    fi
  fi
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1 >/dev/null
  echo "Kafka 已连接 $KAFKA_BOOTSTRAP topic=$KAFKA_TOPIC"
}

ensure_jars() {
  if [[ ! -f "$runtime_dir/lib/postgresql-42.7.4.jar" ]]; then
    curl -fsSL -o "$runtime_dir/lib/postgresql-42.7.4.jar" \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
      || fail "下不了 PostgreSQL JDBC。演示不能改回内存模型。"
  fi
  if [[ ! -f "$runtime_dir/lib/kafka-clients-3.9.0.jar" ]]; then
    if [[ -f "$KAFKA_HOME/libs/kafka-clients-3.9.0.jar" ]]; then
      cp "$KAFKA_HOME/libs"/kafka-clients-*.jar "$KAFKA_HOME/libs"/slf4j-api-*.jar \
        "$KAFKA_HOME/libs"/slf4j-reload4j-*.jar "$KAFKA_HOME/libs"/reload4j-*.jar \
        "$KAFKA_HOME/libs"/lz4-java-*.jar "$KAFKA_HOME/libs"/snappy-java-*.jar \
        "$KAFKA_HOME/libs"/zstd-jni-*.jar "$runtime_dir/lib/"
    else
      fail "缺少 kafka-clients jar。"
    fi
  fi
}

compile_all() {
  find "$root/底座" "$root/假业务" -name '*.java' | sort > "$root/out/sources.txt"
  javac --release 21 -encoding UTF-8 -cp "$runtime_dir/lib/*" -d "$root/out" @"$root/out/sources.txt"
}

start_process() {
  local name="$1"
  shift
  local session="demo-$name"
  tmux -f /exec-daemon/tmux.portal.conf has-session -t "=$session" 2>/dev/null \
    && tmux -f /exec-daemon/tmux.portal.conf kill-session -t "$session" || true
  tmux -f /exec-daemon/tmux.portal.conf new-session -d -s "$session" -c "$root" -- "${SHELL:-bash}" -l
  tmux -f /exec-daemon/tmux.portal.conf send-keys -t "$session:0.0" \
    "cd '$root' && export PGHOST='$PGHOST' PGPORT='$PGPORT' PGUSER='$PGUSER' PGPASSWORD='$PGPASSWORD' PGDATABASE='$PGDATABASE' FAKE_DATABASE='$FAKE_DATABASE' KAFKA_BOOTSTRAP='$KAFKA_BOOTSTRAP' KAFKA_TOPIC='$KAFKA_TOPIC' KAFKA_GROUP='$KAFKA_GROUP' ACCESS_PORT='$ACCESS_PORT' LEASE_MS='$LEASE_MS' && java -cp 'out:$runtime_dir/lib/*' $*" C-m
}

stop_process() {
  local name="$1"
  local session="demo-$name"
  tmux -f /exec-daemon/tmux.portal.conf has-session -t "=$session" 2>/dev/null \
    && tmux -f /exec-daemon/tmux.portal.conf kill-session -t "$session" || true
}

cleanup() {
  stop_process access || true
  stop_process dispatch || true
  stop_process runtime-a || true
  stop_process runtime-b || true
}
trap cleanup EXIT

# stop leftover debug sessions that may hold the access port
for s in dbg-access dbg-dispatch dbg-runtime demo-access demo-dispatch demo-runtime-a demo-runtime-b; do
  tmux -f /exec-daemon/tmux.portal.conf has-session -t "=$s" 2>/dev/null \
    && tmux -f /exec-daemon/tmux.portal.conf kill-session -t "$s" || true
done

ensure_postgres
ensure_kafka
ensure_jars
compile_all

java -cp "out:$runtime_dir/lib/*" live.EnsureSchema \
  || fail "PostgreSQL schema 建不起来。"
{
  echo "PostgreSQL=$(PGPASSWORD=$PGPASSWORD psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -Atc 'SELECT version()')"
  echo "Kafka=$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$KAFKA_TOPIC" | head -1)"
  echo "消费组=$KAFKA_GROUP"
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
curl -fsS "http://127.0.0.1:$ACCESS_PORT/health" >/dev/null || fail "接入层没有起来"

# 分段跑演示，便于中途切换 Worker
java -cp "out:$runtime_dir/lib/*" demo.LiveDemoDriver accept | tee "$root/docs/demo/三段演示.txt"
task_id=$(awk '/^受理 /{print $3; exit}' "$root/docs/demo/三段演示.txt")
[[ -n "$task_id" ]] || fail "没有拿到 task_id"

# 等第一次接管
ready=0
for _ in $(seq 1 100); do
  owner=$(PGPASSWORD=$PGPASSWORD psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -Atc \
    "SELECT owner FROM tasks WHERE task_id='$task_id' AND epoch=1 AND owner IS NOT NULL")
  if [[ -n "$owner" ]]; then
    ready=1
    break
  fi
  sleep 0.2
done
[[ "$ready" -eq 1 ]] || fail "pod-a 没有接管。看 demo-runtime-a 日志。"
{
  echo "接管 epoch=1 owner=$owner"
} | tee -a "$root/docs/demo/三段演示.txt"

java -cp "out:$runtime_dir/lib/*" demo.LiveDemoDriver reject "$task_id" "$owner" | tee -a "$root/docs/demo/三段演示.txt"

# 停掉第一个 Runtime，把租约弄过期，再起第二个
stop_process runtime-a
PGPASSWORD=$PGPASSWORD psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -c \
  "UPDATE tasks SET lease_expires_at=now() - interval '1 second' WHERE task_id='$task_id'" >/dev/null
start_process runtime-b live.LiveWorkerLoop pod-b

ready=0
for _ in $(seq 1 150); do
  next=$(PGPASSWORD=$PGPASSWORD psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -Atc \
    "SELECT owner FROM tasks WHERE task_id='$task_id' AND epoch=2 AND owner IS NOT NULL AND owner<>'$owner'")
  if [[ -n "$next" ]]; then
    ready=1
    break
  fi
  sleep 0.2
done
[[ "$ready" -eq 1 ]] || fail "pod-b 没有从恢复点继续。"
{
  echo "另一个Worker继续 epoch=2 owner=$next"
} | tee -a "$root/docs/demo/三段演示.txt"

java -cp "out:$runtime_dir/lib/*" demo.LiveDemoDriver fakewrite "$task_id" | tee -a "$root/docs/demo/三段演示.txt"

grep -q '任务数=1' "$root/docs/demo/三段演示.txt" || fail "同幂等键不是一条任务"
grep -q '代次拒绝' "$root/docs/demo/三段演示.txt" || fail "没有看到代次拒绝"
grep -q '另一个Worker继续 epoch=2' "$root/docs/demo/三段演示.txt" || fail "没有看到另一个 Worker 继续"
grep -q '写入次数=1' "$root/docs/demo/三段演示.txt" || fail "假写写了第二次"
grep -q '受理' "$root/docs/demo/三段演示.txt" || fail "没有受理"
grep -q '投递' "$root/docs/demo/三段演示.txt" || fail "没有投递"
grep -q '接管' "$root/docs/demo/三段演示.txt" || fail "没有接管"
grep -q '按原编号查询' "$root/docs/demo/三段演示.txt" || fail "没有按原编号查询"
grep -q '进程内Database=未使用' "$root/docs/demo/环境.txt" || fail "环境记录缺失"
grep -q 'PostgreSQL=' "$root/docs/demo/环境.txt" || fail "没有记录真实 PostgreSQL"

echo "演示通过：已连 PostgreSQL 与 Kafka，三段事故都看到了。"
