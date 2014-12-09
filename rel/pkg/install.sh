#!/usr/bin/bash

USER=sniffle
GROUP=$USER

case $2 in
    PRE-INSTALL)
        #if grep '^Image: base64 1[34].[1234].*$' /etc/product
        #then
        #    echo "Image version supported"
        #else
        #    echo "This image version is not supported please use the base64 13.2.1 image."
        #    exit 1
        #fi
        if grep "^$GROUP:" /etc/group > /dev/null 2>&1
        then
            echo "Group already exists, skipping creation."
        else
            echo Creating sniffle group ...
            groupadd $GROUP
        fi
        if id $USER > /dev/null 2>&1
        then
            echo "User already exists, skipping creation."
        else
            echo Creating sniffle user ...
            useradd -g $GROUP -d /var/db/sniffle -s /bin/false $USER
        fi
        echo Creating directories ...
        mkdir -p /var/db/sniffle/ring
        mkdir -p /var/db/sniffle/ipranges
        mkdir -p /var/db/sniffle/packages
        mkdir -p /var/db/sniffle/datasets
        chown -R sniffle:sniffle /var/db/sniffle
        mkdir -p /var/log/sniffle/sasl
        chown -R sniffle:sniffle /var/log/sniffle
        if [ -d /tmp/sniffle ]
        then
            chown -R sniffle:sniffle /tmp/sniffle/
        fi
        ;;
    POST-INSTALL)
        echo Importing service ...
        svccfg import /opt/local/fifo-sniffle/share/sniffle.xml
        echo Trying to guess configuration ...
        IP=`ifconfig net0 | grep inet | awk -e '{print $2}'`
        CONFFILE=/opt/local/fifo-sniffle/etc/sniffle.conf
        if [ ! -f "${CONFFILE}" ]
        then
            cp ${CONFFILE}.example ${CONFFILE}
            sed --in-place -e "s/127.0.0.1/${IP}/g" ${CONFFILE}
            md5sum ${CONFFILE} > ${CONFFILE}.md5
        else
	        /opt/local/fifo-sniffle/share/update_config.sh ${CONFFILE}.example ${CONFFILE} > ${$CONFFILE}.new &&
                mv ${CONFFILE} ${CONFFILE}.old &&
                mv ${CONFFILE}.new ${CONFFILE}
        fi
        ;;
esac
