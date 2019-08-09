#!/usr/bin/env bash

action() {
    local origin="$( pwd )"

    # do nothing when the setup was already done
    if [ "$HLTP_SETUP" = "1" ]; then
        echo "hlt parser already setup"
        return "0"
    fi

    # determine the directory of this file
    if [ ! -z "$ZSH_VERSION" ]; then
        local this_file="${(%):-%x}"
    else
        local this_file="${BASH_SOURCE[0]}"
    fi
    local this_dir="$( dirname "$this_file" )"


    #
    # global variables
    #

    # base directory
    export HLTP_BASE="$( cd "$this_dir" && pwd )"

    # other defaults
    [ -z "$HLTP_STORE" ] && export HLTP_STORE="$HLTP_BASE/store"
    [ -z "$HLTP_SOFTWARE" ] && export HLTP_SOFTWARE="$HLTP_BASE/software"
    [ -z "$HLTP_CMSSW" ] && export HLTP_CMSSW="$HLTP_BASE/cmssw"


    #
    # helper functions
    #

    hltp_add_py() {
        [ ! -z "$1" ] && export PYTHONPATH="$1:$PYTHONPATH"
    }
    [ -z "$ZSH_VERSION" ] && export -f hltp_add_py

    hltp_add_bin() {
        [ ! -z "$1" ] && export PATH="$1:$PATH"
    }
    [ -z "$ZSH_VERSION" ] && export -f hltp_add_bin

    hltp_pip_install() {
        PYTHONUSERBASE="$HLTP_SOFTWARE" pip install -I --user --no-cache-dir "$@"
    }
    [ -z "$ZSH_VERSION" ] && export -f hltp_pip_install

    #
    # setup software
    #

    hltp_install_software() {
        [ "$1" = "force" ] && rm -rf "$HLTP_SOFTWARE"

        if [ ! -d "$HLTP_SOFTWARE" ]; then
            echo "installing sofware in $HLTP_SOFTWARE"
            mkdir -p "$HLTP_SOFTWARE"

            hltp_pip_install --user luigi
            hltp_pip_install six
            hltp_pip_install --no-deps git+https://github.com/riga/law.git
            hltp_pip_install python-telegram-bot
        fi
    }
    [ -z "$ZSH_VERSION" ] && export -f hltp_install_software
    hltp_install_software

    # variables for external software
    export PYTHONWARNINGS="ignore"

    # python software
    local pyv="$( python -c "import sys;print('{0.major}.{0.minor}'.format(sys.version_info))" )"
    hltp_add_py "$HLTP_SOFTWARE/lib/python${pyv}/site-packages"
    hltp_add_bin "$HLTP_SOFTWARE/bin"

    # add _this_ repo to the python path
    hltp_add_py "$HLTP_BASE"


    #
    # law setup
    #

    export LAW_HOME="$HLTP_BASE/.law"
    export LAW_CONFIG_FILE="$HLTP_BASE/law.cfg"

    # source law's bash completion scipt
    source "$( law completion )" ""

    # rerun the task indexing
    law index --verbose


    # remember that the setup run
    export HLTP_SETUP="1"
}
action "$@"
