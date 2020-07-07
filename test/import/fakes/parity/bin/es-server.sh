#!/usr/bin/env sh
TARBALL=elasticsearch-"$ES_VERSION"-darwin-x86_64.tar.gz
TARBALL_FULL_PATH="$(cd "$(dirname "$0")" && pwd || exit 255)"/"$TARBALL"
curl -o "$TARBALL_FULL_PATH" -z "$TARBALL_FULL_PATH" -L https://artifacts.elastic.co/downloads/elasticsearch/"$TARBALL"
cp "$TARBALL_FULL_PATH" "$TMPDIR" || exit 255
cd "$TMPDIR" || exit 255
tar -xvzf ./"$TARBALL" || (rm "$TARBALL_FULL_PATH"; exit 255)
cd ./elasticsearch-"$ES_VERSION"/bin || exit 255
./elasticsearch
