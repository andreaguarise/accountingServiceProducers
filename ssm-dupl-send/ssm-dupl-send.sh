#!/bin/bash

#This script runs apel-client, then it duplicates the ssm records to a second directory path and uses
#ssmsend two times, the first to send records directly to GOC and the second to another broker path.

APELCLIENT_EXECUTABLE=/usr/bin/apelclient
APELCLIENT_CONF=/etc/apel/client.cfg
SSM_EXECUTABLE=/usr/bin/ssmsend
SSM_APEL_CONF=/etc/apel/sender.cfg
SSM_FAUST_CONF=/etc/apel/faust-sender.cfg
APELDIR=/var/spool/apel/outgoing
FAUSTDIR=/var/spool/faust/outgoing

GOON=0
HELP=0

# read the options
TEMP=`getopt -o c:C:s:S:F:d:D:h  --long apelclient:,apelclientconf:,ssmsend:,ssmsendconf:,ssmfaustconf:,apeldir:,faustdir:,help -n 'test.sh' -- "$@"`
eval set -- "$TEMP"

# extract options and their arguments into variables.
while true ; do
    case "$1" in
        -h|--help) HELP=1 ; shift ;;
        -c|--apelclient)
            case "$2" in
                "") shift 2 ;;
                *) APELCLIENT_EXECUTABLE=$2 ; shift 2 ;;
            esac ;;
        -C|--apelclientconf)
            case "$2" in
                "") shift 2 ;;
                *) APELCLIENT_CONF=$2 ; shift 2 ;;
            esac ;;
        -s|--ssmsend)
            case "$2" in
                "") shift 2 ;;
                *) SSM_EXECUTABLE=$2 ; shift 2 ;;
            esac ;;
        -S|--ssmsendconf)
            case "$2" in
                "") shift 2 ;;
                *) SSM_APEL_CONF=$2 ; shift 2 ;;
            esac ;;
        -F|--ssmfaustconf)
            case "$2" in
                "") shift 2 ;;
                *) SSM_FAUST_CONF=$2 ; shift 2 ;;
            esac ;;
        -d|--apeldir)
            case "$2" in
                "") shift 2 ;;
                *) APELDIR=$2 ; shift 2 ;;
            esac ;;
        -D|--faustdir)
            case "$2" in
                "") shift 2 ;;
                *) FAUSTDIR=$2 ; shift 2 ;;
            esac ;;
        --) shift ; break ;;
        *) echo "Internal error!" ; exit 1 ;;
    esac
done

if [ $HELP -eq 1 ]
	echo -n "USAGE: ssm-dupl-send.sh [OPTIONS]
				"
fi

if [ -e $APELCLIENT_EXECUTABLE ]; then
        echo "Using apelclient executable in $APELCLIENT_EXECUTABLE"
else
        echo "Could not find apelclient executable in $APELCLIENT_EXECUTABLE"
        GOON=1
fi

if [ -f $APELCLIENT_CONF ]; then
        echo "Using apelclient config file in $APELCLIENT_CONF"
else
        echo "Could not find apelclient config file in $APELCLIENT_CONF"
        GOON=1
fi

if [ -e $SSM_EXECUTABLE ]; then
        echo "Using ssmsend executable in $SSM_EXECUTABLE"
else
        echo "Could not find ssmsend executable in $SSM_EXECUTABLE"
        GOON=1
fi
if [ -f $SSM_APEL_CONF ]; then
        echo "Using ssm for apel config file in $SSM_APEL_CONF"
else
        echo "Could not find ssm for apel config file in $SSM_APEL_CONF"
        GOON=1
fi
if [ -f $SSM_FAUST_CONF ]; then
        echo "Using ssm for faust config file in $SSM_FAUST_CONF"
else
        echo "Could not find ssm for faust config file in $SSM_FAUST_CONF"
        GOON=1
fi
if [ -d $APELDIR ]; then
        echo "Using apel output dir tree in $APELDIR"
else
        echo "Could not find apel output dir tree in $APELDIR"
        GOON=1
fi
if [ -d $FAUSTDIR ]; then
        echo "Using faust output dir tree in $FAUSTDIR"
else
        echo "Could not find faust output dir tree in $FAUSTDIR"
        GOON=1
fi

if [ $GOON -ne 0 ]; then
        echo "Fix above error first"
        exit 1
fi

##RUN APEL CLIENT (APEL PARSER WAS PREVIOUSLY RUN, SINCE IT COULD ALSO BE RUN ON A DIFFERENT NODE)
$APELCLIENT_EXECUTABLE -c $APELCLIENT_CONF
if [ $? -ne 0 ]; then
        echo "Exiting due to error running: $APELCLIENT_EXECUTABLE -c $APELCLIENT_CONF"
        exit 2
fi

##COPY RECORDS GENERATED BY APEL CLIENT to $FAUSTDIR (USE RSYNC)
rsync -Cav $APELDIR/ $FAUSTDIR
if [ $? -ne 0 ]; then
        echo "Exiting due to error running: rsync -Cav $APELDIR/ $FAUSTDIR"
        exit 3
fi

##INSTANTIATE SSMSEND using apel conf file
$SSM_EXECUTABLE -c $SSM_APEL_CONF
if [ $? -ne 0 ]; then
        echo "Exiting due to error running: $SSM_EXECUTABLE -c $SSM_APEL_CONF"
        exit 4
fi

##INSTANTIATE SSMSEND using faust conf file
$SSM_EXECUTABLE -c $SSM_FAUST_CONF
if [ $? -ne 0 ]; then
        echo "Exiting due to error running: $SSM_EXECUTABLE -c $SSM_FAUST_CONF"
        exit 5
fi

