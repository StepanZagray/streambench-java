#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
jdk=
for candidate in "${JAVA_HOME:-}" /usr/lib/jvm/java-21-openjdk; do
    [ -n "$candidate" ] || continue
    [ -x "$candidate/bin/javac" ] && [ -x "$candidate/bin/java" ] || continue
    version=$("$candidate/bin/javac" -version 2>&1)
    case "$version" in
        "javac 21"|"javac 21."*) jdk=$candidate; break ;;
    esac
done
if [ -z "$jdk" ]; then
    echo 'JDK 21 is required: set JAVA_HOME or install /usr/lib/jvm/java-21-openjdk.' >&2
    exit 1
fi

mkdir -p "$script_dir/build"
"$jdk/bin/javac" --release 21 --add-modules jdk.httpserver -d "$script_dir/build" "$script_dir"/src/*.java
# Preserve the caller's working directory for the DATA_DIR=data default.
exec "$jdk/bin/java" --add-modules jdk.httpserver -cp "$script_dir/build" Streambench
