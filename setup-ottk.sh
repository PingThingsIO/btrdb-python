#!/bin/bash
set -eux

python_version="3.10.11"

ottk_status="$(kubectl get pods -A | grep ottk | grep Running)"
ottk_namespace=$(echo "$ottk_status" | awk '{print $1}')
ottk_pod=$(echo "$ottk_status" | awk '{print $2}')

cat <<EOF | kubectl exec -i -n "$ottk_namespace" "$ottk_pod" -- /bin/bash -c 'cat > /tmp/install-python.sh'
set -eux
apt update -y
apt install -y \
  build-essential zlib1g-dev libncurses5-dev \
  libgdbm-dev libnss3-dev libssl-dev \
  libreadline-dev libffi-dev libsqlite3-dev \
  libbz2-dev
cd /tmp
rm -rf /tmp/python_src
mkdir python_src
cd python_src
curl -L -o python.tgz "https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tgz"
tar -xzf python.tgz
cd Python-*
./configure --prefix=/tmp/python --enable-optimizations
make -j4
make install
cd /tmp
rm -rf /tmp/venv
/tmp/python/bin/python3 -m venv /tmp/venv
. /tmp/venv/bin/activate
pip install 'dask[complete]' pyarrow tqdm
rm -rf ./btrdb-python
git clone https://github.com/PingThingsIO/btrdb-python
cd btrdb-python
pip install -e .
EOF

kubectl exec -n "$ottk_namespace" "$ottk_pod" -- /bin/bash /tmp/install-python.sh
kubectl cp -n "$ottk_namespace" ./parload "$ottk_pod":/tmp/btrdb-python
