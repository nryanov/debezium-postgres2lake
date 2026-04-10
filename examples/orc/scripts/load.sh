#!/usr/bin/env bash
exec python3 "$(dirname "$0")/../../common/scripts/generate_events.py" "$@"
