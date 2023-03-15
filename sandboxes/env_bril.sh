#!/usr/bin/env bash

action() {
    local bril_version="$( law config hltp_config.bril_version )"
    export PATH="${PATH}:/afs/cern.ch/cms/lumi/brilconda-${bril_version}/bin"

    source activate root

    hltp_pip_install() {
        PYTHONUSERBASE="${HLTP_SOFTWARE}" pip install -I --user --no-cache-dir "$@"
    }

    if [ -z "$( type brilcalc 2> /dev/null )" ]; then
        hltp_pip_install -U brilws
    fi
}
action "$@"
