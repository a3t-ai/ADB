# Configuring ADB

Flags set ports and the data directory; everything else is an environment variable
or a live setting changed over HTTP (or in the console's Settings tab).

## Access

| Variable | |
|---|---|
| `ADB_AUTH_TOKEN` | The token for every endpoint except `/health` and `/ui`. Unset: ADB is open (local use only) |
| `ADB_SCOPED_TOKENS` | Tokens limited to namespaces, JSON: `[{"name": "acme", "token": "…", "write": ["acme-*"], "read": ["shared-*"]}]`. They may only run `/query` statements that name a `NAMESPACE` they cover |
| `ADB_METRICS_OPEN` | `1`: `/metrics` needs no token |

## Storage

| Setting | |
|---|---|
| `--data-dir <dir>` | Where a node keeps its data |
| `ADB_COLD_URI` | Object storage for the log and checkpoints: `s3://bucket/prefix`, or `file:///path` for tests. Required for a cluster |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT` | Credentials and endpoint for `s3://` (any S3-compatible store) |

## Embeddings (similarity search)

ADB turns text into vectors in the background. Pick the provider at any time; the
change is applied at once and the vectors are rebuilt in the background when the
model changes:

```bash
curl -s -X PUT http://localhost:3000/admin/embedding -H "Authorization: Bearer $ADB_AUTH_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"provider": "openai", "model": "text-embedding-3-small", "dimension": 1536, "api_key_env": "OPENAI_API_KEY"}'
```

| Field | |
|---|---|
| `provider` | `local` (built-in model, the default), `openai`, or `custom` (any endpoint with the OpenAI embeddings API; set `url`) |
| `model`, `dimension` | The model and its vector size. Local default: `bge-small-en-v1.5`, 384 |
| `api_key_env` | The name of the environment variable holding the API key. ADB never stores the key itself |
| `request_dimension` | `true`: ask the endpoint for `dimension` (models that can shorten vectors) |
| `run_on` | `node` (default) or `compute`: hand the work to compute nodes (`adb serve-compute`, joined with `ADB_COMPUTE_JOIN=http://<node>:3000`) |

The local model is read from `/models/bge-small-en-v1.5`, or `ADB_EMBED_MODEL_DIR`;
`tests/fetch-model.sh <dir>` downloads it. `ADB_EMBED=off` turns embedding off.

## Hot cache

`GET`/`PUT /admin/cache` with `{max_bytes, rules: [{memory, scope?, keep}]}`: which
memory stays in RAM. Applied at once.

## Cluster

A cluster is several nodes sharing object storage. Each node runs `adb serve-http`
with:

| Variable | |
|---|---|
| `ADB_NODE_ID` | This node's number (`1`, `2`, …; `from-hostname` takes it from a StatefulSet pod name) |
| `ADB_CONTROL_PEERS` | The control-plane voters: `1=http://adb-0:3002,2=http://adb-1:3002,3=http://adb-2:3002` |
| `ADB_CONTROL_PORT` | This node's control-plane port (default `3002`; between nodes only) |
| `ADB_ADVERTISE`, `ADB_ADVERTISE_FLIGHT` | The HTTP and Flight addresses other nodes reach this one at |
| `ADB_PARTITIONS` | Memory partitions, fixed for the cluster's life (default 64) |
| `ADB_COLD_URI` | Required |

Kubernetes manifests: [`deploy/k8s`](../deploy/k8s). Clients connect to any node: each
forwards what it does not own.

See also: [Connecting](connect.md).
