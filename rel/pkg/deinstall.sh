#!/usr/bin/bash

case $2 in
    DEINSTALL)
	echo "Stopping Sniffle service."
	svcadm disable network/sniffle
	;;
    POST-DEINSTALL)
	echo "Removing Sniffle service."
	svccfg delete network/sniffle
	echo "Please beware that database and logfiles have not been"
	echo "deleted! Neither have the sniffle user or gorup."
	echo "If you don't need them any more remove the directories:"
	echo " /var/log/sniffle"
	echo " /var/db/sniffle"
	;;
esac
