#!/usr/bin/env bash
S="$(mktemp -d)"; . "${HZ_ENV:?set HZ_ENV to an env file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT, ADB_BUCKET}"
U="${AWS_ENDPOINT}/${ADB_BUCKET}/b1-test"
s3() { curl -s -o "$S/s3body" -D "$S/s3hdr" -w "%{http_code}" --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" "$@"; }
etag() { grep -i '^etag:' "$S/s3hdr" | tr -d '\r' | cut -d' ' -f2-; }
code() { grep -o "<Code>[^<]*</Code>" "$S/s3body" 2>/dev/null | head -1; }
K=probe-$(date +%s)
echo "1 bucket reachable (list)          : HTTP $(s3 "${AWS_ENDPOINT}/${ADB_BUCKET}?list-type=2&max-keys=1") $(code)"
echo "2 plain PUT                        : HTTP $(s3 -X PUT --data-binary 'v1' "$U/$K")"; E1=$(etag); echo "  etag $E1"
echo "3 If-None-Match:* on EXISTING key  : HTTP $(s3 -X PUT -H 'If-None-Match: *' --data-binary 'v2' "$U/$K") $(code)   (want 412)"
echo "4 If-None-Match:* on NEW key       : HTTP $(s3 -X PUT -H 'If-None-Match: *' --data-binary 'n1' "$U/$K-new") $(code)   (want 200)"
echo "5 If-Match: WRONG etag             : HTTP $(s3 -X PUT -H 'If-Match: "deadbeef"' --data-binary 'v3' "$U/$K") $(code)   (want 412)"
echo "6 If-Match: CORRECT etag           : HTTP $(s3 -X PUT -H "If-Match: $E1" --data-binary 'v4' "$U/$K") $(code)   (want 200)"
echo "7 If-Match: STALE etag (v1 again)  : HTTP $(s3 -X PUT -H "If-Match: $E1" --data-binary 'v5' "$U/$K") $(code)   (want 412)"
echo "8 GET content                      : HTTP $(s3 "$U/$K") body=$(cat "$S/s3body")   (want v4)"
echo "$K" > "$S/b1_keys"
