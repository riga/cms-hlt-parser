#!/usr/bin/env bash

action() {
    # do nothing when the setup was already done
    if [ "$HLTP_SETUP" = "1" ]; then
        echo "hlt parser already setup"
        return "0"
    fi


    #
    # local variables
    #

    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local origin="$( pwd )"


    #
    # global variables
    #

    export HLTP_BASE="${this_dir}"
    [ -z "${HLTP_STORE}" ] && export HLTP_STORE="${HLTP_BASE}/store"
    [ -z "${HLTP_SOFTWARE}" ] && export HLTP_SOFTWARE="${HLTP_BASE}/software"
    [ -z "${HLTP_CMSSW}" ] && export HLTP_CMSSW="${HLTP_BASE}/cmssw"

    export PYTHONPATH="${HLTP_BASE}:${PYTHONPATH}"

    export PYTHONWARNINGS="${PYTHONWARNINGS:-ignore}"
    export VIRTUAL_ENV_DISABLE_PROMPT="${VIRTUAL_ENV_DISABLE_PROMPT:-1}"


    #
    # setup software
    #

    hltp_install_software() {
        [ "$1" = "force" ] && rm -rf "${HLTP_SOFTWARE}"

        local missing="$( [ ! -d "${HLTP_SOFTWARE}" ] && echo "true" || echo "false" )"

        if ${missing}; then
            echo "installing sofware in ${HLTP_SOFTWARE}"
            mkdir -p "${HLTP_SOFTWARE}"
            python3 -m venv "${HLTP_SOFTWARE}"
        fi

        source "${HLTP_SOFTWARE}/bin/activate" ""

        if ${missing}; then
            pip install -U pip || return "$?"
            pip install tabulate requests git+https://github.com/riga/law.git || return "$?"
        fi
    }
    [ -z "${ZSH_VERSION}" ] && export -f hltp_install_software

    # install the software stack
    hltp_install_software


    #
    # law setup
    #

    export LAW_HOME="${HLTP_BASE}/.law"
    export LAW_CONFIG_FILE="${HLTP_BASE}/law.cfg"

    # source law's bash completion scipt
    source "$( law completion )" ""

    # rerun the task indexing
    law index --verbose


    # remember that the setup run
    export HLTP_SETUP="1"
}
action "$@"
