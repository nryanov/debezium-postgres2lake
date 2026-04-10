#!/bin/sh
set -eu
cd /work
exec java \
  -Djava.security.manager=allow \
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.io=ALL-UNNAMED \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens java.base/sun.security.util=ALL-UNNAMED \
  --add-opens java.base/sun.security.action=ALL-UNNAMED \
  -Djava.util.logging.manager=org.jboss.logmanager.LogManager \
  ${JVM_OPTS-} \
  -jar /home/jboss/quarkus-run.jar \
  "$@"
