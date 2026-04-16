#!/bin/sh
set -eu
cd /work

SPI_EXT_DIR="${SPI_EXT_DIR:-}"
APP_CP="/home/jboss/*:/home/jboss/lib/*:/home/jboss/lib/main/*:/home/jboss/app:/home/jboss/quarkus-run.jar"
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

if [ -n "$SPI_EXT_DIR" ] && [ -d "$SPI_EXT_DIR" ]; then
  SPI_CP="${SPI_EXT_DIR%/}/*"
  exec sh -c 'exec java '"$JAVA_BASE_ARGS"' ${JVM_OPTS-} -cp "'"$SPI_CP:$APP_CP"'" io.quarkus.bootstrap.runner.QuarkusEntryPoint "$@"' -- "$@"
fi

exec sh -c 'exec java '"$JAVA_BASE_ARGS"' ${JVM_OPTS-} -jar /home/jboss/quarkus-run.jar "$@"' -- "$@"
