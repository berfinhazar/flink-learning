#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
elif [[ -x /usr/libexec/java_home ]]; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
FLINK_DIR="${FLINK_HOME:?Flink kurulum klasörünü FLINK_HOME ile belirtmelisin}"
mvn -q package
"$FLINK_DIR/bin/flink" run -c com.berfin.flink.StockDataAnalysis target/stockdataanaliz-1.0-SNAPSHOT.jar "${1:-$PWD/data/FUTURES_TRADES.txt}" "${2:-$PWD/output}" "${3:-UTC}" "${4:-XYZ}" "${5:-5}" "${6:-0}"
