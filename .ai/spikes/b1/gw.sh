#!/usr/bin/env bash
S="$(mktemp -d)"; . "${HZ_ENV:?set HZ_ENV to an env file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT, ADB_BUCKET}"
for i in $(seq 1 120); do
  c=$(curl -s -o /dev/null -D "$S/gh" -w "%{http_code}" --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" "${AWS_ENDPOINT}/${ADB_BUCKET}/b1-test/mx/fixed")
  gw=$(grep -i '^x-amz-request-id' "$S/gh" | tr -d '\r' | cut -d' ' -f2 | awk -F- '{print $3"-"$6}')
  echo "$gw $c"
done | sort | uniq -c | sort -k2,2 -k3,3 | awk '{printf "gateway %-10s HTTP %s × %s\n", $2, $3, $1}'
