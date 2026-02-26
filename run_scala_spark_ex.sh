#!/bin/bash
# shellcheck disable=SC1091,SC2034

source env_setup.sh

set -ex
set -o pipefail

assembly_target=./core/target/scala-2.13/core-assembly-0.0.2-SNAPSHOT.jar

if [ ! -f  "$assembly_target" ]; then
  sbt assembly
fi

if [ -z "$1" ]; then
  echo "Usage $0 [classname]"
  exit 1
fi



# If this script is used to run a streaming example (e.g. class "com.example.StreamingApp"),
# you'll need a TCP source to feed the stream. The easiest quick-and-dirty way is `netcat`.
# Examples below — pick the one that matches your platform/netcat flavor.

print_netcat_note() {
  cat <<'NOTE'
To test the streaming example use netcat to serve a line-oriented stream.

Linux / OpenBSD netcat (common):
  nc -lk 9999
  -l  = listen
  -k  = keep listening after client disconnect (OpenBSD/netcat-openbsd; many Linux installs)
  (if your nc does not support -k, see the GNU/busybox/ncat examples below)

macOS (if using the built-in OpenBSD-style nc):
  nc -lk 9999

To send test lines into the listener (in another terminal):
  while true; do echo "hello $(date)"; sleep 10; done | nc -lk 9999

Or to run the listener in the background:
  nc -lk 9999 >/dev/null 2>&1 &

If you need a single-shot test (one connection):
  echo "one line" | nc localhost 9999

Replace 9999 with whatever port your streaming example expects.
NOTE
}


if [[ "$1" == *stream* || "$1" == *Stream* ]]; then
  print_netcat_note;
fi

echo "Using $(which spark-submit) to run $1"
spark-submit  --class  "$1" "$assembly_target"
