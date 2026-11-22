#!/bin/bash
# 第 3 章云端验收：真实 PostgreSQL + Kafka + 三个 Java 进程。
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

runtime_dir="$root/.runtime"
compose_file="$root/deploy/demo/docker-compose.yml"
evidence="$root/docs/demo/chapter3-云端.txt"
mkdir -p "$runtime_dir/lib" "$runtime_dir/pids" "$root/docs/demo" "$root/out"

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-runtime}"
export PGPASSWORD="${PGPASSWORD:-runtime}"
export PGDATABASE="${PGDATABASE:-runtime}"
export FAKE_DATABASE="${FAKE_DATABASE:-fake_write}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-runtime.schedule}"
export KAFKA_GROUP="runtime-workers-ch3-$$"
export ACCESS_PORT="${ACCESS_PORT:-8080}"
export LEASE_MS="${DEMO_LEASE_MS:-5000}"
unset DISPATCH_ABORT_AFTER_SEND CRASH_BEFORE_TAKEOVER_COMMIT || true

fail() { echo "第3章云端未通过：$1" >&2; exit 1; }

docker_bin() {
  if docker info >/dev/null 2>&1; then echo docker
  elif sudo docker info >/dev/null 2>&1; then echo "sudo docker"
  else fail "没有可用 Docker"; fi
}
DOCKER=$(docker_bin)
compose() { $DOCKER compose -f "$compose_file" "$@"; }
psql_runtime() {
  compose exec -T postgres env PGPASSWORD="$PGPASSWORD" psql -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 "$@"
}
java_run() { java -cp "out:$runtime_dir/lib/*" "$@"; }

ensure_docker_services() {
  command -v java >/dev/null 2>&1 || fail "没有 java"
  command -v javac >/dev/null 2>&1 || fail "没有 javac"
  compose up -d
  for _ in $(seq 1 60); do
    ready_pg=0; ready_kafka=0
    compose exec -T postgres pg_isready -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1 && ready_pg=1
    compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1 && ready_kafka=1
    [[ "$ready_pg" -eq 1 && "$ready_kafka" -eq 1 ]] && break
    sleep 1
  done
  [[ "$ready_pg" -eq 1 ]] || fail "PostgreSQL 没起来"
  [[ "$ready_kafka" -eq 1 ]] || fail "Kafka 没起来"
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
    --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1 >/dev/null
}

ensure_jars() {
  local base=https://repo1.maven.org/maven2
  download() {
    local url="$1" dest="$2"
    [[ -f "$dest" ]] || curl -fsSL -o "$dest" "$url" || fail "下不了 $dest"
  }
  download "$base/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" "$runtime_dir/lib/postgresql-42.7.4.jar"
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
  local name="$1"; shift
  local pidfile="$runtime_dir/pids/$name.pid"
  local logfile="$runtime_dir/pids/$name.log"
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    sleep 0.5
  fi
  (
    cd "$root"
    export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE FAKE_DATABASE
    export KAFKA_BOOTSTRAP KAFKA_TOPIC KAFKA_GROUP ACCESS_PORT LEASE_MS
    export DISPATCH_ABORT_AFTER_SEND CRASH_BEFORE_TAKEOVER_COMMIT
    exec java -cp "out:$runtime_dir/lib/*" "$@"
  ) >"$logfile" 2>&1 &
  echo $! >"$pidfile"
}

stop_process() {
  local name="$1"
  local pidfile="$runtime_dir/pids/$name.pid"
  if [[ -f "$pidfile" ]]; then
    local pid; pid=$(cat "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
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
  unset DISPATCH_ABORT_AFTER_SEND CRASH_BEFORE_TAKEOVER_COMMIT || true
}
trap cleanup EXIT

wait_access() {
  for _ in $(seq 1 50); do
    curl -fsS "http://127.0.0.1:$ACCESS_PORT/health" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  fail "接入层没起来：$runtime_dir/pids/access.log"
}

ensure_docker_services
ensure_jars
compile_all
java_run live.EnsureSchema || fail "schema 失败"

: > "$evidence"
{
  echo "服务=postgres:16.15:5432 apache/kafka:3.9.0:9092"
  echo "进程=live.LiveAccessServer live.LiveDispatchLoop live.LiveWorkerLoop"
  echo "消费组=$KAFKA_GROUP LEASE_MS=$LEASE_MS"
} | tee -a "$evidence"

# --- 规则 3.2 T1 四样 + 提交前退出 + 事件/消息 ID ---
java_run demo.Chapter3Cloud reset >/dev/null
start_process access live.LiveAccessServer
wait_access
{
  echo "## 规则3.2 T1"
  java_run demo.Chapter3Cloud t1
  java_run demo.Chapter3Cloud conflict
} | tee -a "$evidence"
grep -q '提交前四样 tasks=0 idem=0 events=0 outbox=0' "$evidence" || fail "提交前四样未清空"
grep -q '受理 status=202' "$evidence" || fail "T1 未返回 202"
grep -q '事件ID与消息ID分开 same=false' "$evidence" || fail "事件/消息 ID 未分开"
grep -q '同键异参 status=409' "$evidence" || fail "冲突不是 409"

task_id=$(awk -F= '/^受理 status=202 task_id=/{print $NF; exit}' "$evidence")
[[ -n "$task_id" ]] || fail "没有 task_id"

# --- 规则 3.3 初始 epoch=0，接管后 +1；同消息不推高（部分已在演示）---
{
  echo "## 规则3.3 初始与接管"
  java_run demo.Chapter3Cloud epoch0 "$task_id"
} | tee -a "$evidence"
grep -q '初始 epoch=0 owner=null' "$evidence" || fail "初始 epoch 不是 0"

# 正常投递 + 接管
unset DISPATCH_ABORT_AFTER_SEND
start_process dispatch live.LiveDispatchLoop
start_process runtime-a live.LiveWorkerLoop pod-a
{
  java_run demo.Chapter3Cloud wait-owner "$task_id" pod-a
  java_run demo.Chapter3Cloud inbox "$task_id"
} | tee -a "$evidence"
grep -q '接管 epoch=1 owner=pod-a' "$evidence" || fail "接管失败"
grep -q '消费组非Pod=true' "$evidence" || fail "Inbox 消费方不是消费组"

# --- 规则 3.4 投递器确认前退出，原 message_id 再投 ---
stop_process runtime-a
stop_process dispatch
java_run demo.Chapter3Cloud reset >/dev/null
# 重新受理
key_dispatch="dispatch-abort-$$"
curl -fsS -X POST "http://127.0.0.1:$ACCESS_PORT/tasks" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "tenant=tenant&actor=actor&key=$key_dispatch&digest=digest&budget=100" >/tmp/ch3-accept.txt
task_d=$(sed -n 's/^task_id=//p' /tmp/ch3-accept.txt)
export DISPATCH_ABORT_AFTER_SEND=1
start_process dispatch live.LiveDispatchLoop
# 等确认前退出日志
for _ in $(seq 1 50); do
  grep -q '确认前退出' "$runtime_dir/pids/dispatch.log" 2>/dev/null && break
  sleep 0.2
done
grep -q '确认前退出' "$runtime_dir/pids/dispatch.log" || fail "投递器没有确认前退出"
msg_pending=$(psql_runtime -Atc "SELECT message_id FROM outbox WHERE task_id='$task_d' AND status='PENDING'")
[[ -n "$msg_pending" ]] || fail "确认前退出后 Outbox 应仍 PENDING"
{
  echo "## 规则3.4 投递确认前退出"
  echo "确认前退出后 pending message_id=$msg_pending"
  grep '确认前退出' "$runtime_dir/pids/dispatch.log" | tail -1
} | tee -a "$evidence"
# 关掉 abort，让同一条再投并标记 SENT
stop_process dispatch
unset DISPATCH_ABORT_AFTER_SEND
start_process dispatch live.LiveDispatchLoop
for _ in $(seq 1 50); do
  st=$(psql_runtime -Atc "SELECT status FROM outbox WHERE message_id='$msg_pending'")
  [[ "$st" == "SENT" ]] && break
  sleep 0.2
done
st=$(psql_runtime -Atc "SELECT status FROM outbox WHERE message_id='$msg_pending'")
[[ "$st" == "SENT" ]] || fail "重启后未用原 message_id 标记 SENT"
# Kafka 里应能看到该 message_id（至少一次；再投后可能两次）
kafka_hits=$(compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9092 --topic "$KAFKA_TOPIC" --from-beginning --timeout-ms 5000 2>/dev/null \
  | grep -c "$msg_pending" || true)
{
  echo "再投后 outbox status=$st message_id=$msg_pending kafka命中=$kafka_hits"
} | tee -a "$evidence"
[[ "$kafka_hits" -ge 1 ]] || fail "Kafka 里没有原 message_id"

# --- 规则 3.5 Inbox：消费组 + message_id；重复不增代次 ---
# 当前 task_d 尚未被 worker 接管；起两个 worker 会争抢，但 Inbox 去重后第二次 REUSED
export CRASH_BEFORE_TAKEOVER_COMMIT=
start_process runtime-a live.LiveWorkerLoop pod-a
java_run demo.Chapter3Cloud wait-owner "$task_d" pod-a | tee -a "$evidence"
# 人为再投同一 payload 到 Kafka（模拟重复投递）
payload=$(psql_runtime -Atc "SELECT message_id||'|'||task_id||'|'||dispatch_seq||'|'||expected_epoch||'|'||schedule_type||'|'||run_version FROM outbox WHERE message_id='$msg_pending'")
printf '%s\n' "$payload" | compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:9092 --topic "$KAFKA_TOPIC" >/dev/null
sleep 2
{
  echo "## 规则3.5 Inbox"
  java_run demo.Chapter3Cloud inbox "$task_d"
  java_run demo.Chapter3Cloud task-state "$task_d"
} | tee -a "$evidence"
grep -q 'Inbox行数=1' "$evidence" || fail "重复投递后 Inbox 应仍 1 行"
epoch_after=$(psql_runtime -Atc "SELECT epoch FROM tasks WHERE task_id='$task_d'")
[[ "$epoch_after" == "1" ]] || fail "重复消息推高了代次: $epoch_after"
consumer=$(psql_runtime -Atc "SELECT consumer FROM inbox WHERE message_id='$msg_pending'")
[[ "$consumer" == "$KAFKA_GROUP" ]] || fail "Inbox consumer 不是消费组: $consumer"

# --- 规则 3.6 额度不足延期 ---
stop_process runtime-a
stop_process dispatch
java_run demo.Chapter3Cloud reset >/dev/null
java_run demo.Chapter3Cloud set-quota0 tenant | tee -a "$evidence"
curl -fsS -X POST "http://127.0.0.1:$ACCESS_PORT/tasks" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "tenant=tenant&actor=actor&key=quota-$$&digest=digest&budget=100" >/tmp/ch3-q.txt
task_q=$(sed -n 's/^task_id=//p' /tmp/ch3-q.txt)
start_process dispatch live.LiveDispatchLoop
start_process runtime-a live.LiveWorkerLoop pod-a
{
  echo "## 规则3.6 额度不足"
  java_run demo.Chapter3Cloud defer-outbox "$task_q"
  java_run demo.Chapter3Cloud task-state "$task_q"
  grep -E 'trace=slot$|trace=tx$' "$runtime_dir/pids/runtime-a.log" | head -5
} | tee -a "$evidence"
grep -q '延期 outbox' "$evidence" || fail "没有延期 Outbox"
grep -q 'task state=ACCEPTED epoch=0 owner=null' "$evidence" || fail "额度不足仍授予了 owner"
# 名额先于事务：日志应先 slot 后 tx
slot_line=$(grep -n 'trace=slot$' "$runtime_dir/pids/runtime-a.log" | head -1 | cut -d: -f1)
tx_line=$(grep -n 'trace=tx$' "$runtime_dir/pids/runtime-a.log" | head -1 | cut -d: -f1)
[[ -n "$slot_line" && -n "$tx_line" && "$slot_line" -lt "$tx_line" ]] || fail "本地名额未在开事务前占用"
echo "名额顺序 slot_line=$slot_line tx_line=$tx_line" | tee -a "$evidence"

# --- 规则 3.7 T2 提交前退出可再消费；offset 后靠租约恢复 ---
stop_process runtime-a
stop_process dispatch
java_run demo.Chapter3Cloud reset >/dev/null
curl -fsS -X POST "http://127.0.0.1:$ACCESS_PORT/tasks" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "tenant=tenant&actor=actor&key=t2-$$&digest=digest&budget=100" >/tmp/ch3-t2.txt
task_t2=$(sed -n 's/^task_id=//p' /tmp/ch3-t2.txt)
start_process dispatch live.LiveDispatchLoop
export CRASH_BEFORE_TAKEOVER_COMMIT=1
start_process runtime-a live.LiveWorkerLoop pod-a
for _ in $(seq 1 80); do
  grep -q 'T2提交前退出' "$runtime_dir/pids/runtime-a.log" 2>/dev/null && break
  sleep 0.2
done
grep -q 'T2提交前退出' "$runtime_dir/pids/runtime-a.log" || fail "没有 T2 提交前退出"
epoch_crash=$(psql_runtime -Atc "SELECT epoch FROM tasks WHERE task_id='$task_t2'")
inbox_crash=$(psql_runtime -Atc "SELECT COUNT(*) FROM inbox")
{
  echo "## 规则3.7 T2与恢复"
  echo "T2提交前退出后 epoch=$epoch_crash inbox行数=$inbox_crash"
  grep 'T2提交前退出' "$runtime_dir/pids/runtime-a.log" | tail -1
} | tee -a "$evidence"
[[ "$epoch_crash" == "0" ]] || fail "T2 未提交却留下了代次"
[[ "$inbox_crash" == "0" ]] || fail "T2 未提交却留下了 Inbox"
# 关掉 crash，同一消费组再消费应能接管
stop_process runtime-a
unset CRASH_BEFORE_TAKEOVER_COMMIT
# 新消费组，从 earliest 再读（原消息仍在）
export KAFKA_GROUP="runtime-workers-ch3-retry-$$"
start_process runtime-b live.LiveWorkerLoop pod-b
java_run demo.Chapter3Cloud wait-owner "$task_t2" pod-b | tee -a "$evidence"
grep -q '接管 epoch=1 owner=pod-b' "$evidence" || fail "T2 后未能再接管"
# 杀 worker，等租约自然到期，看恢复登记不增 epoch、新 message_id
stop_process runtime-b
echo "等待租约自然到期…" | tee -a "$evidence"
expired=0
for _ in $(seq 1 120); do
  row=$(psql_runtime -Atc "SELECT CASE WHEN lease_expires_at <= now() THEN 'expired' ELSE 'live' END FROM tasks WHERE task_id='$task_t2'")
  [[ "$row" == "expired" ]] && expired=1 && break
  sleep 0.25
done
[[ "$expired" -eq 1 ]] || fail "租约未自然到期"
# 起一个只做扫描的 runtime（无消费也行，scan 在 loop 里）
export KAFKA_GROUP="runtime-workers-ch3-scan-$$"
start_process runtime-a live.LiveWorkerLoop pod-scan
{
  java_run demo.Chapter3Cloud recovery "$task_t2"
} | tee -a "$evidence"
grep -q '扫描后任务 epoch=1' "$evidence" || fail "恢复扫描推高了代次"
grep -q '新消息ID=true' "$evidence" || fail "恢复没用新 message_id"
# 已有待处理时不重复登记
sleep 1
rec_count=$(psql_runtime -Atc "SELECT COUNT(*) FROM outbox WHERE task_id='$task_t2' AND schedule_type='RECOVERY'")
echo "recovery条数确认=$rec_count" | tee -a "$evidence"
[[ "$rec_count" == "1" ]] || fail "重复登记了恢复 Outbox: $rec_count"

echo "第3章云端规则跑完，证据在 $evidence"
