#!/usr/bin/env bash
# shellcheck disable=1072,1130,1057,1073,1058,1072

# UPGRADE SCRIPT
#
# Description:  This script enables a fully automated upgrade from 4.6.4 to 5.1.1
#
#

# provide the necessary configuration parameters
bin="/opt/couchbase/bin/"
user="Administrator"
pass="password"
cluster="localhost"

# Pull the serverlist  with couchbase-cli, and then iterate through each of the nodes
hosts=$($bin/couchbase-cli server-list -c "$cluster:8091" -u "$user" -p "$pass" | \
        awk "{print $2;}")


# iterate through each server and perform the swap rebalance
for host in $hosts
do

	# Eject node
	$bin/couchbase-cli rebalance -c "$cluster:8091" -u "$user" -p "$pass" --server-remove="$host"

	# wait for rebalance to complete

	# ssh in to ejected node and do the install
    rsync -avq -e "ssh -i /root/.ssh/script_rsa -o StrictHostKeyChecking=no" /etc/hosts "root@$host:/etc/" \
    "wget couchbase-server-architecture__5.1.1.rpm ; \
     rpm -U couchbase-server-architecture__5.1.1.rpm"

    # do a yum / spacewalk upgrade on the machine
    # sudo rhn-channel --list
    # Check the current couchbase channel version
	# sudo rhnreg_ks --serverUrl=https://spacewalk.{datacenter}.fairisaac.com/XMLRPC --activationkey={activationkey} --force
	# Use the activation key of your channel here
	# rhel6 may need to change to rhel7 based on the version of your server
    # <datacenter> can be found from result of this command:
	# sudo cat /etc/sysconfig/rhn/up2date | grep serverURL
	# sudo yum clean all
	# sudo yum search couchbase
    # sudo rhn-channel --list
    # Check for the new couchbase channel with new version

    # Add upgraded/patched node back to cluster (no rebalance yet)
	${bin}/couchbase-cli rebalance -c "$cluster:8091" -u "$user" -p "$pass" --server-add="$host"

done








