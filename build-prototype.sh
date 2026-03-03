#! /usr/bin/env bash

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <tb_path>"
  exit 1
fi

TB_PATH="$1"

upload() {
  echo "Uploading $1 to TerraBlob"
  if ! [ -z ${FORCE+x} ]; then
    tb-cli delete "$TB_PATH"/"$1" || true
  fi
  tb-cli put --timeout 90s --multipart "$1" "$TB_PATH"/"$1"
}

set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

if tb-cli ls "$TB_PATH" > /dev/null 2> /dev/null; then
  if [ -z ${FORCE+x} ] && [[ $(tb-cli ls "$TB_PATH" | wc -l) != 0 ]]; then
    echo "Path $TB_PATH already exists and is non-empty; cannot use"
    exit 1
  elif ! [ -z ${FORCE+x} ]; then
    echo "Path $TB_PATH is non-empty but FORCE is set; proceeding"
  else
    echo "Path $TB_PATH already exists and is empty; can use safely"
  fi
else
  tb-cli mkdir -p "$TB_PATH"
fi


if [ -z ${NOBUILD+x} ]; then

  if [ -z ${NOCLEAN+x} ]; then
    "$SCRIPT_DIR"/gradlew clean
  else
    echo "Skipping clean"
  fi

  "$SCRIPT_DIR"/gradlew :connect:uber:{spotlessApply,build} releaseTarGz -x test

else
  echo "Skipping build"
fi

if ! [ -z ${NOPUSH+x} ]; then
  echo "Skipping upload"
  exit 0
fi


pushd "$SCRIPT_DIR"/core/build/distributions > /dev/null
upload kafka_2.13-4.3.0-SNAPSHOT.tgz
popd > /dev/null

pushd "$SCRIPT_DIR"/connect/uber/build > /dev/null
TEMP_DIR="$(mktemp -d)"
mkdir "$TEMP_DIR"/connect-uber-plugins
cp libs/* dependant-libs/* "$TEMP_DIR"/connect-uber-plugins
pushd "$TEMP_DIR" > /dev/null
tar czf connect-uber-plugins.tgz connect-uber-plugins
upload connect-uber-plugins.tgz
popd > /dev/null
popd > /dev/null

echo "Finished uploading artifacts to $TB_PATH"
