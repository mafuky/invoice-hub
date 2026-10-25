#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."
scenario=${SCENARIO:-scenarios/chaos.yaml}
count=100
timeout=10m
while [ "$#" -gt 0 ]; do
    case "$1" in
        -n|-scenario|-timeout)
            [ "$#" -ge 2 ] || { echo "crash-demo: missing option value" >&2; exit 2; }
            case "$1" in
                -n) count=$2 ;;
                -scenario) scenario=$2 ;;
                -timeout) timeout=$2 ;;
            esac
            shift 2 ;;
        *) echo "usage: crash-demo.sh [-n 100] [-scenario scenarios/chaos.yaml] [-timeout 10m]" >&2; exit 2 ;;
    esac
done
case "$count" in ''|*[!0-9]*|0) echo "crash-demo: n must be positive" >&2; exit 2 ;; esac

work=$(mktemp -d "${TMPDIR:-/tmp}/invoice-hub-crash.XXXXXX")
services=(api invoice-svc provider-svc delivery-svc settlement-svc)
pids=("" "" "" "" "")
mock_pid=""
demo_pid=""

cleanup() {
    local pid
    for pid in "$demo_pid" "$mock_pid" "${pids[@]}"; do
        if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi
    done
    local deadline=$((SECONDS + 10))
    local alive
    while [ "$SECONDS" -lt "$deadline" ]; do
        alive=0
        for pid in "$demo_pid" "$mock_pid" "${pids[@]}"; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then alive=1; fi
        done
        [ "$alive" -eq 1 ] || break
        sleep 1
    done
    for pid in "$demo_pid" "$mock_pid" "${pids[@]}"; do
        if [ -n "$pid" ]; then
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    echo "crash-demo: logs and summary in $work" >&2
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Build first so tracked PIDs are service processes, not go run wrappers.
for service in "${services[@]}" mock-supplier demo reconcile; do
    go build -o "$work/$service" "./cmd/$service"
done
start_service() {
    local index=$1
    local service=${services[$index]}
    if [ "$service" = api ]; then
        "$work/$service" >>"$work/$service.log" 2>&1 &
    else
        "$work/$service" serve >>"$work/$service.log" 2>&1 &
    fi
    pids[$index]=$!
}

"$work/mock-supplier" -scenario "$scenario" >"$work/mock-supplier.log" 2>&1 &
mock_pid=$!
for index in 0 1 2 3 4; do start_service "$index"; done

since=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
"$work/demo" -n "$count" -scenario "$scenario" -timeout "$timeout" >"$work/summary.json" 2>"$work/demo.log" &
demo_pid=$!
next_crash=$((SECONDS + 20))
service_failed=0
while kill -0 "$demo_pid" 2>/dev/null; do
    for index in 0 1 2 3 4; do
        if ! kill -0 "${pids[$index]}" 2>/dev/null; then
            echo "crash-demo: ${services[$index]} exited; check its log and make tunnel" >&2
            service_failed=1
            break
        fi
    done
    if ! kill -0 "$mock_pid" 2>/dev/null; then service_failed=1; fi
    if [ "$service_failed" -eq 1 ]; then kill "$demo_pid" 2>/dev/null || true; break; fi
    if [ "$SECONDS" -ge "$next_crash" ]; then
        index=$((RANDOM % 5))
        echo "crash-demo: killing and restarting ${services[$index]}" >&2
        kill -9 "${pids[$index]}" 2>/dev/null || true
        wait "${pids[$index]}" 2>/dev/null || true
        start_service "$index"
        next_crash=$((SECONDS + 20))
    fi
    sleep 1
done
demo_code=0
wait "$demo_pid" || demo_code=$?
demo_pid=""
if [ -s "$work/summary.json" ]; then
    cat "$work/summary.json"
    run_since=$(sed -n 's/.*"started_at":"\([^"]*\)".*/\1/p' "$work/summary.json")
    if [ -n "$run_since" ]; then since=$run_since; fi
fi
reconcile_code=0
"$work/reconcile" -since "$since" || reconcile_code=$?
if [ "$demo_code" -ne 0 ] || [ "$reconcile_code" -ne 0 ] || [ "$service_failed" -ne 0 ]; then exit 1; fi
