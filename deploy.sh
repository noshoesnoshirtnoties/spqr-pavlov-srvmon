#!/bin/bash

VERSION=1.2.0
SUBJECT=deploy
USAGE="Usage: $0 -d dsthost -u sshuser -n -v\n
-d destination host\n
-u ssh/scp user\n
-n no cronjob\n
-v verbose output"

# --- options processing -------------------------------------------

if [ $# == 0 ] ; then
    echo -e $USAGE
    exit 1;
fi

while getopts ":d:u:n:v" optname
  do
    case "$optname" in
      "v")
        echo "[INFO] verbose mode active"
        VERBOSE=true
        ;;
      "d")
        DSTHOST=$OPTARG
        ;;
      "u")
        SSHUSER=$OPTARG
        ;;
      "n")
        NOCRONJOB=true
        ;;
      "?")
        echo "[ERROR] unknown option $OPTARG - exiting"
        exit 1;
        ;;
      ":")
        echo "[ERROR] no argument value for option $OPTARG - exiting"
        exit 1;
        ;;
      *)
        echo "[ERROR] unknown error while processing options - exiting"
        exit 1;
        ;;
    esac
  done

shift $(($OPTIND - 1))

# --- body --------------------------------------------------------

read -s -n 1 -p "[WAIT] press any key to continue..." && echo ""
if [ $VERBOSE ]; then echo "[INFO] starting deployment"; fi

if [ $VERBOSE ]; then echo "[INFO] setting defaults"; fi
SSH="$(which ssh) -q -o StrictHostKeyChecking=no -A -F /home/${USER}/.ssh/config -l ${SSHUSER} "
SCP="$(which scp) -F /home/${USER}/.ssh/config "
SRVMONPATH="/home/steam/spqr-pavlov-srvmon"
SRVMONUSER="steam"
SRVMONLOG="${SRVMONPATH}/spqr-pavlov-srvmon.log"
FILES=(
  "meta.json"
  "config.json"
  "main.py"
  "srvmon.py"
  "generate-ranks.cron.py"
)

if [ ! -n "${DSTHOST}" ]; then
  echo "[ERROR] given destination host is invalid - exiting"; exit 1
fi

if [ ! -n "${SSHUSER}" ]; then
  echo "[ERROR] given ssh user is invalid - exiting"; exit 1
fi

if [ $VERBOSE ]; then echo "[INFO] stopping the server"; fi
$SSH $DSTHOST "/usr/bin/systemctl stop spqr-pavlov-srvmon.service"
sleep 3

if [ $VERBOSE ]; then echo "[INFO] installing dependencies"; fi
$SSH $DSTHOST "sudo su steam -c 'pip install async-pavlov mysql-connector'"

if [ $VERBOSE ]; then echo "[INFO] checking if service user exists"; fi
RESPONSE=$($SSH $DSTHOST "grep '^steam:' /etc/passwd")
if [ -z $RESPONSE ]; then
  if [ $VERBOSE ]; then echo "[INFO] could not find service user - trying to create it"; fi
  $SSH $DSTHOST "useradd -m steam"
fi

if [ $VERBOSE ]; then echo "[INFO] creating service home"; fi
$SSH $DSTHOST "mkdir -p /home/steam/spqr-pavlov-srvmon"

if [ $VERBOSE ]; then echo "[INFO] transferring files"; fi
for FILE in "${FILES[@]}"; do
  $SCP "${FILE}" ${SSHUSER}@${DSTHOST}:${SRVMONPATH}/${FILE}
  $SSH $DSTHOST "/usr/bin/chmod 664 ${SRVMONPATH}/${FILE}; /usr/bin/chown ${SRVMONUSER}:${SRVMONUSER} ${SRVMONPATH}/${FILE}"
done

if [ $NOCRONJOB == false ]; then
  $SCP "generate-ranks.cron.sh" "${SSHUSER}@${DSTHOST}:/etc/cron.d/generate-ranks.cron.sh"
  $SSH $DSTHOST "/usr/bin/chmod 775 /etc/cron.d/generate-ranks.cron.sh; /usr/bin/chown steam:steam /etc/cron.d/generate-ranks.cron.sh"
fi

$SCP "spqr-pavlov-srvmon.service" "${SSHUSER}@${DSTHOST}:/etc/systemd/system/spqr-pavlov-srvmon.service"
$SSH $DSTHOST "/usr/bin/chmod 664 /etc/systemd/system/spqr-pavlov-srvmon.service; /usr/bin/chown root:root /etc/systemd/system/spqr-pavlov-srvmon.service"

if [ $VERBOSE ]; then echo "[INFO] creating empty logfile for service"; fi
$SSH $DSTHOST "touch ${SRVMONLOG}; /usr/bin/chown ${SRVMONUSER}:${SRVMONUSER} ${SRVMONLOG}"

if [ $VERBOSE ]; then echo "[INFO] enabling the systemd service"; fi
$SSH $DSTHOST "/usr/bin/systemctl enable spqr-pavlov-srvmon.service"

if [ $VERBOSE ]; then echo "[INFO] starting the systemd service"; fi
$SSH $DSTHOST "/usr/bin/systemctl start spqr-pavlov-srvmon.service"
sleep 3
$SSH $DSTHOST "/usr/bin/systemctl status spqr-pavlov-srvmon.service | grep 'Active:'"

if [ $VERBOSE ]; then echo "[INFO] exiting without errors"; fi

exit 0
