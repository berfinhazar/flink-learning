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
"$FLINK_DIR/bin/flink" run -c com.berfin.flink.BankFraudAnalysis target/bankarealdataanaliz-1.0-SNAPSHOT.jar "${1:-$PWD/data/course}" "${2:-$PWD/output/course-alerts.csv}" "${3:-Asia/Kolkata}" "${4:-300}"
