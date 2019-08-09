#!/usr/bin/env bash

action() {
    export PATH="$PATH:/afs/cern.ch/cms/lumi/brilconda-1.1.7-cc7/bin"

    source activate root

    if [ -z "$( type brilcalc 2> /dev/null )" ]; then
        hltp_pip_install -U brilws
    fi
}
action "$@"
