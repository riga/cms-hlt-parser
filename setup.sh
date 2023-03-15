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

    # base directory
    export HLTP_BASE="${this_dir}"

    # other defaults
    [ -z "${HLTP_STORE}" ] && export HLTP_STORE="${HLTP_BASE}/store"
    [ -z "${HLTP_SOFTWARE}" ] && export HLTP_SOFTWARE="${HLTP_BASE}/software"
    [ -z "${HLTP_CMSSW}" ] && export HLTP_CMSSW="${HLTP_BASE}/cmssw"


    #
    # helper functions
    #

    hltp_add_py() {
        [ ! -z "$1" ] && export PYTHONPATH="$1:${PYTHONPATH}"
    }
    [ -z "${ZSH_VERSION}" ] && export -f hltp_add_py

    hltp_add_bin() {
        [ ! -z "$1" ] && export PATH="$1:${PATH}"
    }
    [ -z "${ZSH_VERSION}" ] && export -f hltp_add_bin

    hltp_pip_install() {
        PYTHONUSERBASE="${HLTP_SOFTWARE}" pip3 install -I --user --no-cache-dir "$@"
    }
    [ -z "${ZSH_VERSION}" ] && export -f hltp_pip_install


    #
    # setup software
    #

    hltp_install_software() {
        [ "$1" = "force" ] && rm -rf "${HLTP_SOFTWARE}"

        if [ ! -d "${HLTP_SOFTWARE}" ]; then
            echo "installing sofware in ${HLTP_SOFTWARE}"
            mkdir -p "${HLTP_SOFTWARE}"

            hltp_pip_install pip || return "$?"
            hltp_pip_install six || return "$?"
            hltp_pip_install tabulate || return "$?"
            hltp_pip_install requests || return "$?"
            hltp_pip_install luigi || return "$?"
            LAW_INSTALL_EXECUTABLE="/usr/bin/env python3" hltp_pip_install --no-deps git+https://github.com/riga/law.git || return "$?"
        fi
    }
    [ -z "${ZSH_VERSION}" ] && export -f hltp_install_software

    # variables for external software
    export PYTHONWARNINGS="ignore"

    # python software
    local pyv="$( python3 -c "import sys;print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    hltp_add_py "${HLTP_SOFTWARE}/lib/python${pyv}/site-packages"
    hltp_add_bin "${HLTP_SOFTWARE}/bin"

    # install the software stack
    hltp_install_software

    # add _this_ repo to the python path
    hltp_add_py "${HLTP_BASE}"


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
