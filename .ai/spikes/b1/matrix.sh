#!/usr/bin/env bash
S="$(mktemp -d)"; . "${HZ_ENV:?set HZ_ENV to an env file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT, ADB_BUCKET}"
U="${AWS_ENDPOINT}/${ADB_BUCKET}/b1-test/mx"
req() { curl -s -o "$S/mb" -D "$S/mh" -w "%{http_code}" --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" "$@"; }
etag() { req -I "$1" >/dev/null; grep -i '^etag:' "$S/mh" | tr -d '\r' | cut -d' ' -f2-; }
N=${N:-30}; base=$(date +%s)
req -X PUT --data-binary 'seed' "$U/fixed" >/dev/null
declare -A R
for i in $(seq 1 $N); do
  R[A_plainPUT_new]+="$(req -X PUT --data-binary "a$i" "$U/a-$base-$i") "
  R[B_plainGET]+="$(req "$U/fixed") "
  R[C_INM_new_want200]+="$(req -X PUT -H 'If-None-Match: *' --data-binary "c$i" "$U/c-$base-$i") "
  R[D_INM_exist_want412]+="$(req -X PUT -H 'If-None-Match: *' --data-binary "d$i" "$U/fixed") "
  e=$(etag "$U/fixed")
  R[E_IfMatch_correct_quoted_want200]+="$(req -X PUT -H "If-Match: $e" --data-binary "seed" "$U/fixed") "
  e=$(etag "$U/fixed"); eu=${e//\"/}
  R[F_IfMatch_correct_unquoted_want200]+="$(req -X PUT -H "If-Match: $eu" --data-binary "seed" "$U/fixed") "
  R[G_IfMatch_wrong_want412]+="$(req -X PUT -H 'If-Match: "0000"' --data-binary "g$i" "$U/fixed") "
done
for k in $(printf '%s\n' "${!R[@]}" | sort); do printf "%-36s %s\n" "$k" "$(echo ${R[$k]} | tr ' ' '\n' | sort | uniq -c | awk '{printf "%s×%s  ", $2, $1}')"; done
