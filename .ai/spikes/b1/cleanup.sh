#!/usr/bin/env bash
S="$(mktemp -d)"; . "${HZ_ENV:?set HZ_ENV to an env file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT, ADB_BUCKET}"
req() { curl -s -w "\n%{http_code}" --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" "$@"; }
deleted=0; failed=0
for pass in 1 2 3 4 5 6; do
  out=""; for t in 1 2 3 4 5; do out=$(req "${AWS_ENDPOINT}/${ADB_BUCKET}?list-type=2&prefix=b1-test/&max-keys=1000"); [ "${out##*$'\n'}" = 200 ] && break; done
  keys=$(echo "${out%$'\n'*}" | grep -o '<Key>[^<]*</Key>' | sed 's/<Key>//;s/<\/Key>//')
  [ -z "$keys" ] && break
  for k in $keys; do
    for t in 1 2 3 4 5; do c=$(req -X DELETE "${AWS_ENDPOINT}/${ADB_BUCKET}/$k" | tail -1); [ "$c" = 204 ] || [ "$c" = 200 ] && break; done
    if [ "$c" = 204 ] || [ "$c" = 200 ]; then deleted=$((deleted+1)); else failed=$((failed+1)); fi
  done
done
left=$(req "${AWS_ENDPOINT}/${ADB_BUCKET}?list-type=2&prefix=b1-test/&max-keys=10" | grep -o '<KeyCount>[0-9]*</KeyCount>')
echo "deleted=$deleted failed=$failed remaining: $left"
