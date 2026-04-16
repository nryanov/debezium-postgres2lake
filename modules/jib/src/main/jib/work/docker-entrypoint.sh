#!/bin/sh
set -eu
cd /work

SPI_EXT_DIR="${SPI_EXT_DIR:-}"
APP_MAIN_CLASS="${APP_MAIN_CLASS:-}"
APP_CP="/app/resources:/app/classes:/app/libs/*"
JAVA_BASE_ARGS="\
  -Djava.security.manager=allow \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens java.base/sun.security.util=ALL-UNNAMED \
  --add-opens java.base/sun.security.action=ALL-UNNAMED \
  -Djava.util.logging.manager=org.jboss.logmanager.LogManager"

if [ ! -d "/app/classes" ] || [ ! -d "/app/libs" ]; then
  echo "Legacy Quarkus layout expected under /app (missing /app/classes or /app/libs)." >&2
  exit 1
fi

if [ -n "$SPI_EXT_DIR" ] && [ -d "$SPI_EXT_DIR" ]; then
  if [ -z "$APP_MAIN_CLASS" ]; then
    echo "SPI classpath mode requires APP_MAIN_CLASS for legacy-jar startup." >&2
    exit 1
  fi
  SPI_CP="${SPI_EXT_DIR%/}/*"
  exec sh -c 'exec java '"$JAVA_BASE_ARGS"' ${JVM_OPTS-} -cp "'"$SPI_CP:$APP_CP"'" "'"$APP_MAIN_CLASS"'" "$@"' -- "$@"
fi

if [ -n "$APP_MAIN_CLASS" ]; then
  exec sh -c 'exec java '"$JAVA_BASE_ARGS"' ${JVM_OPTS-} -cp "'"$APP_CP"'" "'"$APP_MAIN_CLASS"'" "$@"' -- "$@"
fi

RUNNER_JAR="$(ls /app/*-runner.jar 2>/dev/null | sed -n '1p')"
if [ -n "$RUNNER_JAR" ]; then
  exec sh -c 'exec java '"$JAVA_BASE_ARGS"' ${JVM_OPTS-} -jar "'"$RUNNER_JAR"'" "$@"' -- "$@"
fi

echo "Cannot start legacy-jar app: set APP_MAIN_CLASS or provide /app/*-runner.jar." >&2
exit 1
