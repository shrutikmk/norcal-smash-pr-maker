#!/usr/bin/env bash
# Start or stop the PR Maker local stack (vLLM + Python API + Vite) from one shell.
#
# Designed to run alongside the scheduler stack: reuses vLLM on :8000 when already
# up, and binds the PR Maker API on :8775 (scheduler web UI uses :8765).
#
# Usage:
#   ./scripts/pr-maker-local-stack.sh start
#   ./scripts/pr-maker-local-stack.sh stop
#   ./scripts/pr-maker-local-stack.sh restart
#   ./scripts/pr-maker-local-stack.sh status
#   ./scripts/pr-maker-local-stack.sh logs
#   ./scripts/pr-maker-local-stack.sh run
#
# Env:
#   PR_MAKER_SKIP_VLLM=1       — never start vLLM (requires VLLM_14B_BASE_URL or probe on PR_MAKER_VLLM_PORT)
#   PR_MAKER_SKIP_WEB=1        — API + vLLM only (run Vite manually)
#   PR_MAKER_VLLM_PORT=8000
#   PR_MAKER_VLLM_MODEL        — default ~/models/gemma-4-26B-A4B
#   PR_MAKER_VLLM_SERVED_NAME  — default gemma-4-26B-A4B
#   PR_MAKER_VLLM_EXTRA_ARGS    — default --reasoning-parser gemma4
#   PR_MAKER_API_PORT=8775     — default 8775 (not 8765; scheduler uses that)
#   PR_MAKER_WEB_PORT=5173
#   PR_MAKER_VLLM_WAIT_SEC=600
#   PR_MAKER_API_WAIT_SEC=60
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATEDIR="$ROOT/scripts/.pr-maker-stack"
PIDFILE="$STATEDIR/pids"
LOGDIR="$STATEDIR/logs"

export HF_HOME="${HF_HOME:-$HOME/models/.hf-cache}"

VLLM_PORT="${PR_MAKER_VLLM_PORT:-8000}"
VLLM_MODEL="${PR_MAKER_VLLM_MODEL:-$HOME/models/gemma-4-26B-A4B}"
VLLM_SERVED_NAME="${PR_MAKER_VLLM_SERVED_NAME:-gemma-4-26B-A4B}"
VLLM_EXTRA_ARGS="${PR_MAKER_VLLM_EXTRA_ARGS:---reasoning-parser gemma4}"
API_PORT="${PR_MAKER_API_PORT:-8775}"
WEB_PORT="${PR_MAKER_WEB_PORT:-5173}"
VLLM_WAIT_SEC="${PR_MAKER_VLLM_WAIT_SEC:-600}"
API_WAIT_SEC="${PR_MAKER_API_WAIT_SEC:-60}"

API_HOST="127.0.0.1"
VLLM_REUSED=0

python_bin() {
  if [[ -n "${PR_MAKER_PYTHON:-}" && -x "${PR_MAKER_PYTHON}" ]]; then
    echo "${PR_MAKER_PYTHON}"
  elif [[ -x "$ROOT/.venv/bin/python" ]]; then
    echo "$ROOT/.venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    command -v python3
  else
    echo "python3"
  fi
}

wait_http() {
  local url=$1
  local msg=$2
  local max_sec=$3
  local label=${4:-$url}
  local child_pid=${5:-}
  local waited=0
  local next_progress=30
  echo "Waiting for $label (timeout ${max_sec}s; model load can take several minutes) ..."
  while [[ "$waited" -lt "$max_sec" ]]; do
    if [[ -n "$child_pid" ]] && ! kill -0 "$child_pid" 2>/dev/null; then
      echo "error: process (pid $child_pid) exited before $label responded." >&2
      return 1
    fi
    if curl -sf --connect-timeout 2 --max-time 15 -o /dev/null "$url" 2>/dev/null; then
      echo "$msg"
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
    if [[ "$waited" -ge "$next_progress" ]]; then
      echo "  ... still waiting (${waited}s / ${max_sec}s)"
      next_progress=$((next_progress + 30))
    fi
  done
  echo "error: timed out after ${max_sec}s waiting for $label ($url)" >&2
  return 1
}

vllm_http_up() {
  curl -sf --connect-timeout 2 --max-time 15 \
    "http://127.0.0.1:${VLLM_PORT}/v1/models" >/dev/null 2>&1
}

port_listener_pids() {
  local port=$1
  lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
}

ensure_vllm_env() {
  export VLLM_14B_BASE_URL="${VLLM_14B_BASE_URL:-http://127.0.0.1:${VLLM_PORT}/v1}"
  export VLLM_14B_MODEL="${VLLM_14B_MODEL:-$VLLM_SERVED_NAME}"
}

probe_existing_vllm() {
  ensure_vllm_env
  echo "Checking for existing vLLM on port ${VLLM_PORT} ..."
  if ! vllm_http_up; then
    return 1
  fi
  echo "vLLM already active on port ${VLLM_PORT} (e.g. scheduler stack); reusing it."
  VLLM_REUSED=1
  if ! "$(python_bin)" "$ROOT/tools/llm_client.py" --diagnose-only; then
    echo "error: vLLM on port ${VLLM_PORT} responded but health check failed." >&2
    exit 1
  fi
  return 0
}

start_vllm_if_needed() {
  ensure_vllm_env

  if [[ "${PR_MAKER_SKIP_VLLM:-0}" == "1" ]]; then
    if [[ -z "${VLLM_14B_BASE_URL:-}" ]]; then
      echo "error: PR_MAKER_SKIP_VLLM=1 requires VLLM_14B_BASE_URL (e.g. http://127.0.0.1:8000/v1)" >&2
      exit 1
    fi
    if ! vllm_http_up; then
      echo "error: PR_MAKER_SKIP_VLLM=1 but vLLM is not responding on port ${VLLM_PORT}." >&2
      exit 1
    fi
    echo "Using external vLLM at ${VLLM_14B_BASE_URL} (PR_MAKER_SKIP_VLLM=1)."
    VLLM_REUSED=1
    "$(python_bin)" "$ROOT/tools/llm_client.py" --diagnose-only
    return 0
  fi

  if probe_existing_vllm; then
    return 0
  fi

  if ! command -v vllm >/dev/null 2>&1; then
    echo "error: vllm not on PATH and nothing is listening on port ${VLLM_PORT}." >&2
    echo "  Start the scheduler stack or vLLM manually, or install vLLM Metal." >&2
    exit 1
  fi

  echo "Starting vLLM on port $VLLM_PORT (log: $LOGDIR/vllm.log) ..."
  # shellcheck disable=SC2086
  nohup vllm serve "$VLLM_MODEL" --port "$VLLM_PORT" --served-model-name "$VLLM_SERVED_NAME" \
    $VLLM_EXTRA_ARGS \
    >>"$LOGDIR/vllm.log" 2>&1 &
  vllm_pid=$!
  echo "vllm $vllm_pid" >>"$PIDFILE"
  sleep 3
  if ! kill -0 "$vllm_pid" 2>/dev/null; then
    echo "error: vLLM exited right after start. See $LOGDIR/vllm.log" >&2
    tail -n 80 "$LOGDIR/vllm.log" >&2 || true
    cmd_stop || true
    exit 1
  fi
  wait_http \
    "http://127.0.0.1:${VLLM_PORT}/v1/models" \
    "vLLM is up." \
    "$VLLM_WAIT_SEC" \
    "vLLM OpenAI API on port ${VLLM_PORT}" \
    "$vllm_pid" || {
    echo "vLLM log tail ($LOGDIR/vllm.log):" >&2
    tail -n 60 "$LOGDIR/vllm.log" 2>/dev/null || true
    cmd_stop || true
    exit 1
  }
  "$(python_bin)" "$ROOT/tools/llm_client.py" --diagnose-only || {
    cmd_stop || true
    exit 1
  }
}

stop_pidfile_processes() {
  local lines
  if [[ ! -f "$PIDFILE" ]]; then
    return 0
  fi
  echo "Stopping PR Maker processes from $PIDFILE ..."
  lines=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -n "$line" ]] && lines+=("$line")
  done <"$PIDFILE"
  for ((idx = ${#lines[@]} - 1; idx >= 0; idx--)); do
    read -r name pid <<<"${lines[idx]}"
    if kill -0 "$pid" 2>/dev/null; then
      echo "  SIGTERM $name (pid $pid)"
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  sleep 1
  for ((idx = ${#lines[@]} - 1; idx >= 0; idx--)); do
    read -r name pid <<<"${lines[idx]}"
    if kill -0 "$pid" 2>/dev/null; then
      echo "  SIGKILL $name (pid $pid)"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
  rm -f "$PIDFILE"
}

cmd_status() {
  if [[ ! -f "$PIDFILE" ]]; then
    echo "No pidfile at $PIDFILE (stack not started via this script)."
    if vllm_http_up; then
      echo "Note: vLLM is responding on port ${VLLM_PORT} (may be shared with scheduler)."
    fi
    return 1
  fi
  local ok=0
  while read -r name pid; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "$name pid $pid: running"
    else
      echo "$name pid $pid: not running"
      ok=1
    fi
  done <"$PIDFILE"
  if vllm_http_up; then
    echo "vLLM on port ${VLLM_PORT}: responding"
  fi
  return "$ok"
}

cmd_stop() {
  stop_pidfile_processes
  echo "Stop complete (only PR Maker pidfile processes were stopped; shared vLLM left running)."
}

cmd_restart() {
  echo "Restarting stack (stop then start) ..."
  cmd_stop || true
  cmd_start
}

cleanup_run() {
  stop_pidfile_processes
  if [[ -n "${tail_pid:-}" ]]; then
    kill -TERM "$tail_pid" 2>/dev/null || true
  fi
}

cmd_run() {
  local wait_pids
  local p
  mkdir -p "$LOGDIR"
  trap cleanup_run INT TERM EXIT
  PR_MAKER_STACK_FOREGROUND=1 cmd_start
  echo "Tailing logs (Ctrl+C stops the stack). Logs: $LOGDIR"
  if [[ "${PR_MAKER_SKIP_WEB:-0}" == "1" ]]; then
    if [[ "$VLLM_REUSED" -eq 1 ]]; then
      tail -n 0 -f "$LOGDIR/api.log" & tail_pid=$!
    else
      tail -n 0 -f "$LOGDIR/vllm.log" "$LOGDIR/api.log" & tail_pid=$!
    fi
  else
    if [[ "$VLLM_REUSED" -eq 1 ]]; then
      tail -n 0 -f "$LOGDIR/api.log" "$LOGDIR/web.log" & tail_pid=$!
    else
      tail -n 0 -f "$LOGDIR/vllm.log" "$LOGDIR/api.log" "$LOGDIR/web.log" & tail_pid=$!
    fi
  fi
  wait_pids=()
  for p in "${vllm_pid:-}" "${api_pid:-}" "${web_pid:-}"; do
    [[ -n "$p" ]] && wait_pids+=("$p")
  done
  if [[ ${#wait_pids[@]} -gt 0 ]]; then
    wait "${wait_pids[@]}" 2>/dev/null || true
  fi
}

cmd_start() {
  local any_alive=0
  local existing
  mkdir -p "$LOGDIR" "$STATEDIR"
  touch "$LOGDIR/api.log"
  if [[ "${PR_MAKER_SKIP_WEB:-0}" != "1" ]]; then
    touch "$LOGDIR/web.log"
  fi

  if [[ -f "$PIDFILE" ]]; then
    any_alive=0
    while read -r name pid; do
      [[ -z "$pid" ]] && continue
      if kill -0 "$pid" 2>/dev/null; then
        any_alive=1
        break
      fi
    done <"$PIDFILE"
    if [[ "$any_alive" -eq 1 ]]; then
      echo "Stack already running (see $PIDFILE). Stop first or run: $0 stop" >&2
      exit 1
    fi
  fi
  rm -f "$PIDFILE"

  vllm_pid=""
  api_pid=""
  web_pid=""
  VLLM_REUSED=0

  start_vllm_if_needed

  existing="$(port_listener_pids "$API_PORT")"
  if [[ -n "$existing" ]]; then
    echo "error: port ${API_PORT} is already in use (PID ${existing})." >&2
    echo "  Scheduler web UI uses 8765; PR Maker API defaults to 8775." >&2
    echo "  Set PR_MAKER_API_PORT to another free port if needed." >&2
    cmd_stop || true
    exit 1
  fi

  echo "Starting API on $API_HOST:$API_PORT (log: $LOGDIR/api.log) ..."
  nohup "$(python_bin)" "$ROOT/tools/web_api.py" --host "$API_HOST" --port "$API_PORT" \
    >>"$LOGDIR/api.log" 2>&1 &
  api_pid=$!
  echo "api $api_pid" >>"$PIDFILE"
  sleep 2
  if ! kill -0 "$api_pid" 2>/dev/null; then
    echo "error: API exited right after start. See $LOGDIR/api.log" >&2
    tail -n 80 "$LOGDIR/api.log" >&2 || true
    cmd_stop || true
    exit 1
  fi
  wait_http \
    "http://${API_HOST}:${API_PORT}/api/health" \
    "API is up." \
    "$API_WAIT_SEC" \
    "PR Maker API on ${API_HOST}:${API_PORT}" \
    "$api_pid" || {
    echo "API log tail ($LOGDIR/api.log):" >&2
    tail -n 60 "$LOGDIR/api.log" 2>/dev/null || true
    cmd_stop || true
    exit 1
  }

  if [[ "${PR_MAKER_SKIP_WEB:-0}" != "1" ]]; then
    existing="$(port_listener_pids "$WEB_PORT")"
    if [[ -n "$existing" ]]; then
      echo "error: port ${WEB_PORT} is already in use (PID ${existing})." >&2
      echo "  Set PR_MAKER_WEB_PORT or stop the other dev server." >&2
      cmd_stop || true
      exit 1
    fi
    if ! command -v npm >/dev/null 2>&1; then
      echo "error: npm not on PATH (set PR_MAKER_SKIP_WEB=1 to skip Vite)" >&2
      cmd_stop || true
      exit 1
    fi
    echo "Starting Vite on port $WEB_PORT (log: $LOGDIR/web.log) ..."
    nohup env PR_MAKER_API_PORT="$API_PORT" VITE_API_PORT="$API_PORT" \
      npm --prefix "$ROOT/web" run dev -- --host 127.0.0.1 --port "$WEB_PORT" \
      >>"$LOGDIR/web.log" 2>&1 &
    web_pid=$!
    echo "web $web_pid" >>"$PIDFILE"
    wait_http \
      "http://127.0.0.1:${WEB_PORT}/" \
      "Vite is up." \
      120 \
      "Vite dev server on port ${WEB_PORT}" \
      "$web_pid" || {
      echo "Web log tail ($LOGDIR/web.log):" >&2
      tail -n 60 "$LOGDIR/web.log" 2>/dev/null || true
      cmd_stop || true
      exit 1
    }
  fi

  echo ""
  echo "Stack started."
  if [[ "${PR_MAKER_SKIP_WEB:-0}" != "1" ]]; then
    echo "  Web UI:  http://127.0.0.1:${WEB_PORT}/"
  fi
  echo "  API:     http://${API_HOST}:${API_PORT}/"
  if [[ "$VLLM_REUSED" -eq 1 ]]; then
    echo "  vLLM:    ${VLLM_14B_BASE_URL} (reused — shared with scheduler or another stack)"
  else
    echo "  vLLM:    ${VLLM_14B_BASE_URL}"
  fi
  echo "  Logs:    $LOGDIR"
  echo "  Stop:    $0 stop"
  echo "  Restart: $0 restart"
  if [[ -z "${PR_MAKER_STACK_FOREGROUND:-}" ]]; then
    echo "  Tail logs: $0 logs"
  fi
}

cmd_logs() {
  if [[ ! -d "$LOGDIR" ]]; then
    echo "No log dir yet ($LOGDIR). Run $0 start first." >&2
    exit 1
  fi
  touch "$LOGDIR/api.log"
  if [[ "${PR_MAKER_SKIP_WEB:-0}" == "1" ]]; then
    if [[ -f "$LOGDIR/vllm.log" ]]; then
      tail -n 50 -f "$LOGDIR/vllm.log" "$LOGDIR/api.log"
    else
      tail -n 50 -f "$LOGDIR/api.log"
    fi
  else
    touch "$LOGDIR/web.log"
    if [[ -f "$LOGDIR/vllm.log" ]]; then
      tail -n 50 -f "$LOGDIR/vllm.log" "$LOGDIR/api.log" "$LOGDIR/web.log"
    else
      tail -n 50 -f "$LOGDIR/api.log" "$LOGDIR/web.log"
    fi
  fi
}

usage() {
  sed -n '2,24p' "$0" | sed 's/^# *//'
}

main() {
  local sub=${1:-}
  case "$sub" in
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    logs) cmd_logs ;;
    run) cmd_run ;;
    restart) cmd_restart ;;
    -h | --help | help) usage ;;
    *)
      echo "usage: $0 {start|stop|status|logs|run|restart}" >&2
      exit 1
      ;;
  esac
}

main "$@"
