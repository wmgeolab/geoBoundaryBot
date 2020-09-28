#!/bin/sh -l

echo ${{ inputs.check }}
apt-get update
apt-get install -y software-properties-common
add-apt-repository ppa:git-core/ppa
apt-get update
apt-get install -y git
curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash
apt-get install git-lfs
  