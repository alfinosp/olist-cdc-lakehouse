#!/bin/sh
# Give an arbitrary runtime uid a passwd entry; Hadoop's login code fails without one.
set -e
if ! whoami >/dev/null 2>&1; then
    echo "spark:x:$(id -u):$(id -g):spark:${HOME}:/bin/sh" >> /etc/passwd
fi
exec "$@"
