#!/usr/bin/env bash

# Default variable values
FIX=false

## colours
red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "Options:"
  echo " --help   Display this help message"
  echo " --fix    Apply the fixes, don't just check them! (default is to check only)"
}

handle_options() {
  while [ $# -gt 0 ]; do
    case $1 in
      --help)
        usage
        exit 0
        ;;
      --fix)
        FIX=true
        ;;
      *)
        echo "Invalid option: $1" >&2
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# Main script execution
handle_options "$@"


if [ "$FIX" = true ]
then
  # Note: not all linting tools provide fix options.
  echo "Running ${mag}ruff check${end} fix..."
  python -m ruff check --fix-only

  echo "Running ${mag}ruff format${end} fix..."
  python -m ruff format
else
 
  echo "Running ${mag}ruff check${end}..."
  python -m ruff check
  ruff_check_exit_code=$?
 
  echo "Running ${mag}ruff format${end}..."
  python -m ruff format --check
  ruff_format_exit_code=$?

  echo "Running ${mag}mypy${end} check..."
  python -m mypy .
  mypy_exit_code=$?

  echo "${cyn}ruff_check_exit_code: $ruff_check_exit_code"
  echo "${cyn}ruff_format_exit_code: $ruff_format_exit_code"
  echo "mypy_exit_code: $mypy_exit_code"

  if [ "$ruff_check_exit_code" == "0" ] && [ "$ruff_format_exit_code" == "0" ] && [ "$mypy_exit_code" == "0" ]
  then
    exit 0
  else
    exit 1
  fi
 
fi
