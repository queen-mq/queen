#!/bin/bash
# GCE startup script for the queen-bench VMs: the operator's key for root (the harness ssh's as root).
mkdir -p /root/.ssh
chmod 700 /root/.ssh
KEY='__PUBKEY__'
grep -qF "$KEY" /root/.ssh/authorized_keys 2>/dev/null || echo "$KEY" >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
