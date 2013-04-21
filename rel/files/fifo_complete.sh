#!/usr/bin/env bash

_fifoadm_complete_generic() {
    fifoadm ${COMP_WORDS[1]} list | tail +3 | awk '{print $1}'
}

_fifoadm_complete_group() {
    fifoadm groups list | tail +3 | awk '{print $2}'
}

_fifoadm_complete_user() {
    fifoadm users list | tail +3 | awk '{print $2}'
}

#  list [-j]
#  get [-j] <uuid>
#  logs [-j] <uuid>
#  snapshots [-j] <uuid>
#  snapshot <uuid> <comment>
#  start <uuid>
#  stop <uuid>
#  reboot <uuid>
#  delete <uuid>
_fifoadm_vms() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="list get logs snapshots snapshots start stop reboot delete"
    if [[ ${COMP_CWORD} == 2 ]] ;
    then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    else
        case "${COMP_WORDS[2]}" in
            list)
                if [[ ${cur} == -* ]]
                then
                    opts="-j"
                    COMPREPLY=( $(compgen -W "-j" -- ${cur}) )
                    return 0
                fi
                ;;
            start|stop|reboot|delete|snapshot)
                opts=`_fifoadm_complete_generic`
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
            get|logs|snapshots)
                if [[ ${cur} == -* ]]
                then
                    opts="-j"
                else
                    opts=`_fifoadm_complete_generic`
                fi
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
        esac
    fi
}

#  list [-j]
#  get [-j] <uuid>
#  delete <uuid>

_fifoadm_generic() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="list get delete"
    if [[ ${COMP_CWORD} == 2 ]] ;
    then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    else
        case "${COMP_WORDS[2]}" in
            list)
                if [[ ${cur} == -* ]]
                then
                    opts="-j"
                    COMPREPLY=( $(compgen -W "-j" -- ${cur}) )
                    return 0
                fi
                ;;
            delete)
                opts=`_fifoadm_complete_generic`
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
            get)
                if [[ ${cur} == -* ]]
                then
                    opts="-j"
                else
                    opts=`_fifoadm_complete_generic`
                fi
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
        esac
    fi
}

_fifoadm_importable() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="list get delete import"
    if [[ ${COMP_CWORD} == 2 ]] ;
    then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    else
        case "${COMP_WORDS[2]}" in
            import)
                opts=`ls ${cur}*`
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
            list)
                if [[ ${cur} == -* ]]
                then
                    opts="-j"
                    COMPREPLY=( $(compgen -W "-j" -- ${cur}) )
                    return 0
                fi
                ;;
            delete)
                opts=`_fifoadm_complete_generic`
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
            get)
                if [[ ${cur} == -* ]]
                then
                    opts="-j"
                else
                    opts=`_fifoadm_complete_generic`
                fi
                COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                return 0
                ;;
        esac
    fi
}

#  list [-j]
#  get [-j] <uuid>
#  delete <uuid>

_fifoadm_users() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="list add join leave passwd grant revoke"
    if [[ ${COMP_CWORD} == 2 ]] ;
    then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    else
        case "${COMP_WORDS[2]}" in
            join|leave)
                if [[ ${COMP_CWORD} == 3 ]]
                then
                    opts=`_fifoadm_complete_user`
                    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                    return 0
                elif [[ ${COMP_CWORD} == 4 ]]
                then
                    opts=`_fifoadm_complete_group`
                    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                    return 0
                fi
                ;;
            passwd|grant|revoke)
                if [[ ${COMP_CWORD} == 3 ]]
                then
                    opts=`_fifoadm_complete_user`
                    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                    return 0
                fi
                ;;
        esac
    fi
}

_fifoadm_groups() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="list grant revoke"
    if [[ ${COMP_CWORD} == 2 ]] ;
    then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    else
        case "${COMP_WORDS[2]}" in
            grant|revoke)
                if [[ ${COMP_CWORD} == 3 ]]
                then
                    opts=`_fifoadm_complete_group`
                    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                    return 0
                fi
                ;;
        esac
    fi
}

#  snarl:   groups, users
#  sniffle: vms, hypervisors, packages, datasets, networks, dtrace
#  general: help
_fifoadm()
{
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="groups users vms hypervisors packages datasets networks dtrace help"
    if [[ ${COMP_CWORD} == 1 ]] ; then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    else
        case "${COMP_WORDS[1]}" in
            vms)
                _fifoadm_vms
                ;;
            networks|hypervisors|packages)
                _fifoadm_generic
                ;;
            datasets|dtrace)
                _fifoadm_importable
                ;;
            users)
                _fifoadm_users
                ;;
            groups)
                _fifoadm_groups
                ;;
        esac

    fi
}
complete -F _fifoadm fifoadm
