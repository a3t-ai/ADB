#!/usr/bin/env bash
# Fetch ADB's embedding model (W4): BAAI/bge-small-en-v1.5 as ONNX (Hugging Face
# Xenova/bge-small-en-v1.5) into <dir> (default $BUILD_DIR/models/bge-small-en-v1.5).
# Used by the image build (baked at /models/bge-small-en-v1.5) and by tests
# (ADB_EMBED_MODEL_DIR). Idempotent.
set -euo pipefail
DIR="${1:-${BUILD_DIR:-/var/tmp/a3t-build}/models/bge-small-en-v1.5}"
BASE="https://huggingface.co/Xenova/bge-small-en-v1.5/resolve/main"
mkdir -p "$DIR"
fetch() {
    local from="$1" to="$2"
    [ -s "$DIR/$to" ] && return
    curl -fsSL --retry 3 -o "$DIR/$to.part" "$BASE/$from" && mv "$DIR/$to.part" "$DIR/$to"
}
fetch onnx/model.onnx model.onnx
for f in tokenizer.json config.json special_tokens_map.json tokenizer_config.json; do fetch "$f" "$f"; done
echo "$DIR"
