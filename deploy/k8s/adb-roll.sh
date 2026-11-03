#!/bin/sh
# Roll the ADB replica set (StatefulSet `adb`, updateStrategy OnDelete) to an image:
# replace each follower and wait until it is back in the group, then the leader
# last (one leader change). Usage: adb-roll.sh <image> [namespace]
set -eu
IMAGE=$1
NS=${2:-a3t}
status() { kubectl get --raw "/api/v1/namespaces/$NS/pods/$1:3002/proxy/cluster/status" --request-timeout=3s 2>/dev/null || true; }
state() { status "$1" | sed -n 's/.*"state":"\([A-Za-z]*\)".*/\1/p'; }
leader() { for p in $PODS; do [ "$(state "$p")" = Leader ] && echo "$p" && return; done; }
member() { for i in $(seq 120); do case "$(state "$1")" in Leader|Follower) return 0;; esac; sleep 2; done; echo "$1 did not rejoin" >&2; return 1; }
image_of() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.containers[0].image}'; }

kubectl -n "$NS" set image statefulset/adb adb="$IMAGE"
PODS=$(kubectl -n "$NS" get pods -l app=adb -o jsonpath='{.items[*].metadata.name}')
L=$(leader)
[ -n "$L" ] || { echo "no leader: fix the replica set before rolling" >&2; exit 1; }
for p in $PODS; do
  [ "$p" = "$L" ] && continue
  [ "$(image_of "$p")" = "$IMAGE" ] && { echo "$p already on $IMAGE"; continue; }
  echo "replacing follower $p"
  kubectl -n "$NS" delete pod "$p"
  sleep 3; member "$p"
done
if [ "$(image_of "$L")" != "$IMAGE" ]; then
  echo "replacing leader $L"
  kubectl -n "$NS" delete pod "$L"
  sleep 3; member "$L"
fi
sleep 5
for p in $PODS; do echo "$p $(image_of "$p") $(status "$p" | grep -o '"state":"[A-Za-z]*"\|"term":[0-9]*\|"wal_offset":[0-9]*' | tr '\n' ' ')"; done
