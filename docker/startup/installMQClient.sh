#!/bin/bash

cd ~
mkdir MQ_Client

#untar the MQ client installable
tar -xvf /tmp/9.1.0.15-IBM-MQC-UbuntuLinuxX64.tar.gz -C MQ_Client

# accept license
./MQ_Client/mqlicense.sh -accept

echo "deb [trusted=yes] file:~/MQ_Client ./" > /etc/apt/sources.list.d/ibmmq-install.list

apt update
cd MQ_Client
ls

# install the minimum required MQ packages
apt install ./ibmmq-runtime_9.1.0.15_amd64.deb
apt install ./ibmmq-client_9.1.0.15_amd64.deb
apt install ./ibmmq-sdk_9.1.0.15_amd64.deb

# add the user to mqm group
#adduser $USER mqm

# source mq environment
. /opt/mqm/bin/setmqenv -s

# create symbolic links to MQ commands
#ln -s /opt/mqm/lib64/libmq* /usr/lib64/
#ln -s /opt/mqm/lib/libmq* /usr/lib/

# If you enconter error - ImportError: libmqic_r.so:
echo "/opt/mqm/lib64" >> /etc/ld.so.conf.d/libc.conf
echo "/opt/mqm/lib" >> /etc/ld.so.conf.d/libc.conf
ldconfig

echo "export PATH=$PATH:/opt/mqm/bin" >> ~/.profile

# cleanup the installable
cd ..
rm -rf MQ_Client
rm -f /tmp/9.1.0.15-IBM-MQC-UbuntuLinuxX64.tar.gz