#!/bin/bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

title() {
  echo -e "\033[1m${1}\033[0m"
}

usage() {
  echo "A script used to compare OpenLineage events generated on two different versions of the same producer."
  echo "Currently, only files with JSON events are supported. Each OpenLineage event has to be a separate line in the file."
  echo "In future, a script will be able to extract lineage events from logs if console tranpsort is used."
  echo
  title "USAGE:"
  echo "  ./$(basename -- "${0}") --prev PREV_VERSION_EVENTS_FILE --next NEXT_VERSION_EVENTS_FILE"
  echo
  title "EXAMPLES:"
  echo "  $ ./ol-diff.sh --prev examples/failure/prev.txt --next examples/failure/next.txt "
  echo "  $ ./ol-diff.sh --prev examples/success/prev.txt --next examples/success/next.txt "
  echo "  $ ./ol-diff.sh --prev examples/success/prev.txt --next examples/success/next.txt --config config.yml "
  echo
  title "ARGUMENTS:"
  echo "  --prev string     file with OpenLineage events produced by the previous version of the connector"
  echo "  --next string     file with OpenLineage events produced by the previous version of the connector"
  echo "  --config string   yaml configuration file"
  exit 1
}

# (1) Parse arguments
while [ $# -gt 0 ]; do
  case $1 in
    --prev)
       shift
       PREV="${1}"
       ;;
    --next)
       shift
       NEXT="${1}"
       ;;
    --config)
       shift
       CONF="${1}"
       ;;
    -h|--help)
       usage
       ;;
    *) usage
       ;;
  esac
  shift
done

if test -z "$PREV"
then
    echo "prev argument can't be empty"
    echo
    usage
    exit 1
fi

if test -z "$NEXT"
then
    echo "next argument can't be empty"
    echo
    usage
    exit 1
fi

docker run --rm -u gradle -v "$PWD":/home/gradle/project -w /home/gradle/project gradle:jdk17-ubi  gradle clean test -Pprev.path=$PREV -Pnext.path=$NEXT -Pconfig=$CONF
open build/reports/tests/test/index.html