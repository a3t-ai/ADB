#!/usr/bin/env bash
S="$(mktemp -d)"; . "${HZ_ENV:?set HZ_ENV to an env file with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT, ADB_BUCKET}"
U="${AWS_ENDPOINT}/${ADB_BUCKET}/b1-test/race"
ROUNDS=${ROUNDS:-100}; W=${W:-16}; base=$(date +%s)
put() { curl -s -o /dev/null -w "%{http_code}" --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" -X PUT -H 'If-None-Match: *' --data-binary "writer-$2" "$U/$1"; }
get() { curl -s -w "\n%{http_code}" --aws-sigv4 "aws:amz:${AWS_REGION}:s3" --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" "$U/$1"; }
export -f put; export S U AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
multi=0; none=0; mismatch=0; ok=0; tot403=0; tot412=0; tot200=0; other=0
for r in $(seq 1 $ROUNDS); do
  key="k-$base-$r"
  res=$(seq 1 $W | xargs -P $W -I{} bash -c 'echo "{}:$(put '"$key"' {})"')
  wins=$(echo "$res" | grep -c ':200$'); n403=$(echo "$res" | grep -c ':403$'); n412=$(echo "$res" | grep -c ':412$')
  tot200=$((tot200+wins)); tot403=$((tot403+n403)); tot412=$((tot412+n412)); other=$((other + W - wins - n403 - n412))
  if [ "$wins" -gt 1 ]; then multi=$((multi+1)); echo "ROUND $r: $wins winners!"; fi
  if [ "$wins" -eq 1 ]; then
    winner=$(echo "$res" | grep ':200$' | cut -d: -f1)
    for t in 1 2 3; do out=$(get "$key"); code=${out##*$'\n'}; [ "$code" = 200 ] && break; done
    body=${out%$'\n'*}
    if [ "$body" = "writer-$winner" ]; then ok=$((ok+1)); else mismatch=$((mismatch+1)); echo "ROUND $r: winner $winner but stored '$body' ($code)"; fi
  fi
  [ "$wins" -eq 0 ] && none=$((none+1))
done
echo "rounds=$ROUNDS writers/round=$W"
echo "requests: 200=$tot200  412=$tot412  403=$tot403  other=$other"
echo "rounds with exactly one winner, stored body correct: $ok"
echo "rounds with >1 winner (SAFETY VIOLATION): $multi"
echo "rounds where stored body != winner (SAFETY VIOLATION): $mismatch"
echo "rounds with no winner (all writers got 403): $none"
