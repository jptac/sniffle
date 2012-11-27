#!/usr/bin/bash

USER=sniffle
GROUP=$USER

case $2 in
    PRE-INSTALL)
	if grep "^$GROUP" /etc/group > /dev/null 2>&1
	then
	    echo "Group already exists, skipping creation."
	else
	    echo Creating snarl group ...
	    groupadd $GROUP
	fi
	if id $USER > /dev/null 2>&1
	then
	    echo "User already exists, skipping creation."
	else
	    echo Creating snarl user ...
	    useradd -g $GROUP -d /var/db/snarl -s /bin/false $USER
	fi
	echo Creating directories ...
	mkdir -p /var/db/sniffle/ring
	mkdir -p /var/db/sniffle/ipranges
	mkdir -p /var/db/sniffle/packages
	mkdir -p /var/db/sniffle/datasets
	chown -R sniffle:sniffle /var/db/sniffle
	mkdir -p /var/log/sniffle/sasl
	chown -R sniffle:sniffle /var/log/sniffle
	;;
    POST-INSTALL)
	echo Importing service ...
	svccfg import /opt/local/sniffle/etc/sniffle.xml
	echo Trying to guess configuration ...
	IP=`ifconfig net0 | grep inet | awk -e '{print $2}'`
	sed --in-place=.bak -e "s/127.0.0.1/${IP}/g" /opt/local/sniffle/etc/vm.args
	sed --in-place=.bak -e "s/127.0.0.1/${IP}/g" /opt/local/sniffle/etc/app.config
	;;
esac
