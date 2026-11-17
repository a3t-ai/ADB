# Spike B1 — Conditional writes on Hetzner Object Storage

> **Date:** 5 Oct 2026 · **Endpoint:** `nbg1.your-objectstorage.com` (Ceph RGW,
> `nbg1-prod1-ceph5`) · **Bucket:** `adb1` · tracks `RISK_BURNDOWN.md` B1
> **Verdict:** ☑ **Closed — passes for Lance's commit path**, with two findings that
> become design rules and one operational issue to clear before production.

## Question

Lance commits (and ADB's object-store commit mode) need compare-and-swap on object
storage. Does Hetzner enforce it, atomically, under concurrency?

## What Lance actually needs

Lance 2.0's S3 commit handler is `ConditionalPutCommitHandler`
(`lance-table/src/io/commit.rs`), which writes each new manifest version with
`object_store::PutMode::Create` → `If-None-Match: *` ("create only if absent").
It does **not** use `If-Match` replace. `object_store` 0.12.5's default
`S3ConditionalPut::ETagMatch` maps `PutMode::Create` to exactly that header.

## Results

### 1. Conditional create (`If-None-Match: *`) — **works, atomically**

| Test | Rounds × writers | Exactly one winner, stored body = winner | Safety violations | Losers |
|---|---|---|---|---|
| Raw S3 (curl, SigV4) | 100 × 16 | **100 / 100** | **0** | 1,279 × `412 PreconditionFailed` |
| `object_store` 0.12.5 (`PutMode::Create`) | 50 × 16 | **50 / 50** | **0** | 614 × `AlreadyExists` (correct mapping) |

Sequential checks: create on a new key → 200; on an existing key → 412.

### 2. Conditional replace (`If-Match: <etag>`) — **unusable**

| Test (30 tries each, correct current ETag) | Result |
|---|---|
| `If-Match` with the ETag **quoted**, exactly as the server returns it | 24 × false `412`, 4 × 200, 2 × 403 |
| `If-Match` with the ETag **unquoted** | 27 × 200, 3 × 403 |
| `object_store` `PutMode::Update` with the `e_tag` just returned | **0 / 20 succeeded** (14 × false 412, 6 × 403) |
| `If-Match` with a wrong ETag | 412 (correct) |

Ceph RGW here appears to compare `If-Match` against the bare ETag, while standard
clients send it quoted (RFC 9110). Replace-style CAS therefore fails spuriously.

### 3. Three gateways reject this access key — **operational**

About 14–17% of **all** requests (plain GET/PUT included) returned
`403 AccessDenied` with an empty message. Correlating `x-amz-request-id` with
outcome (120 GETs, ~20 gateways):

| Gateway (request-id segment) | Outcome |
|---|---|
| `1703e95d`, `24c81324`, `27a73953` | **always 403** |
| every other gateway | always 200 |

Deterministic per gateway → those three RGW instances don't recognise the key
(typical of a newly created key not yet synced, or a stale auth cache). Not
throttling, not related to conditional writes. Example request IDs:
`tx00000f3fbff20fa696ef0-006ac3f895-1703e95d-nbg1-prod1-ceph5`.

## Consequences (design rules)

1. **Lance on Hetzner is viable**: its commit is create-only, which is atomic here.
2. **ADB must never depend on `If-Match` replace on Hetzner.** Anything that needs
   CAS uses **create-only** objects:
   - object-store commit mode: one immutable log object per batch, e.g.
     `log/p{partition}/{offset:020}`, created with `If-None-Match: *`; the next
     offset is claimed by creating it, never by updating a "head" object;
   - fencing/leases on object storage: create `lease/p{partition}/{epoch}`, never
     overwrite;
   - the offset guard (§9.3) lives inside the Lance commit, which is create-based.
3. **Clients must retry `403` on this provider until the gateway issue is fixed**
   (`object_store` does not retry 403). Better: get it fixed — a production commit
   path cannot tolerate one gateway in seven refusing it.

## Follow-ups

| ID | Action | Owner |
|---|---|---|
| B1a | ☑ **Resolved.** Re-checked about an hour after the spike: 120/120 requests succeeded across all gateways, no 403s. It was propagation of the new key to every gateway. Lesson: after creating a key, wait for propagation (or run `gw.sh`) before benchmarking. | @Sriram |
| B1b | Ask Hetzner whether `If-Match` on PUT is supported/expected to honour quoted ETags (RFC 9110). Informational — ADB doesn't depend on it. | @Sriram |
| B1c | Rotate the access key used for this spike (it was shared in a chat session). | @Sriram |

## Re-run on fsn1 (6 Oct 2026) — bucket `adb2`, the ADB spike bucket from now on

| Test | Result |
|---|---|
| Raw S3 race, 100 rounds × 16 writers, `If-None-Match: *` | **100/100 exactly one winner, stored body correct, 0 violations** — create-only is atomic on fsn1 too |
| Status codes across 320 racing PUTs | 20 × 200, 268 × 412, **32 × 404 `NoSuchBucket`** |
| Bucket list × 150, by gateway | 18 gateways always 200; **2 gateways (`511edb6c`, `4dd1bdac`) always `NoSuchBucket`** |

The 404s are the same class of problem as B1a, for a new **bucket** instead of a new
key: some gateways haven't learned it yet. Lance treats a 404 on a manifest listing as
"dataset not found", so no Lance benchmark runs until every gateway answers 200
(tracked as B1d).

Rule from B1a + B1d: **after creating a key or a bucket, wait until `gw.sh` shows every
gateway answering 200 before benchmarking.**

## How to re-run

Scripts are in [`b1/`](b1/). They read credentials from an env file you supply —
never commit it:

```bash
cat > ~/hz.env <<'EOF'   # chmod 600
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
export AWS_REGION=nbg1  AWS_ENDPOINT=https://nbg1.your-objectstorage.com  ADB_BUCKET=adb1
EOF
HZ_ENV=~/hz.env bash b1/probe.sh          # single-shot semantics
HZ_ENV=~/hz.env N=30 bash b1/matrix.sh    # 30× per case, outcome counts
HZ_ENV=~/hz.env bash b1/race.sh           # 100 rounds × 16 concurrent creates
HZ_ENV=~/hz.env bash b1/gw.sh             # 403s per gateway
(. ~/hz.env; cd b1/object-store-race && cargo run --release)   # via object_store
HZ_ENV=~/hz.env bash b1/cleanup.sh        # delete b1-test/
```

All test objects (214) were deleted after the run.
