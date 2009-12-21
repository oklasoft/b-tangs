#!/usr/bin/env bash

awk '{printf( "%s%s", $0, (NR%4 ? "\t" : "\n") ) }' "$@"