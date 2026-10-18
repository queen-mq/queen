#!/bin/bash
# GCE startup script for the queen-bench VMs: the operator's key for root (the harness ssh's as root).
mkdir -p /root/.ssh
chmod 700 /root/.ssh
KEY='ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDEcNQEaXyzsZPuOJqDSPZSL4jDPD7Ik+Zt6kvJ95dkWE2YteQhG5OkNVFgmGKeQNgZ0kURM9feeLyYtwNNG1Lynttb9pqo1u93CH+cRuxNo/cAHsgcMd6KAQ9d1k4+AnBS8cFhfvsNu1cWkdAACGDyKuNpu7EGdmXSYW2s0nxfNa6K+L/JK3e8xqK+oM3wnT+8re0AdHqLQUKjekDmmCFRUuDFYggGcrDW14YxN+sGiJC7Gc/Kx1UK/09FJfhurBTrWQvcrvtF6WriVdy90rHcrVPw08wTxDhK/+qkSNW4W/k1n5d2iPRFpkWbBlSdpONCH0EgGN5QEqRdAtIXEtbl9iTG8MXNmOmbkhQ0kGYVxDJWnAKuweLLP+una8pV8gMorNdDEnAXix5B6AbKZnVpZOGTiiglfQYRO8GCVO6LV3EnPXkrxWbFp1nwuCxw6mIB1nbGmp6j4pVi9llKzkOJ7ZyBzrujfP5Ewr6EjQWUoovRfEPMD2VLrl8hpqX4+A8= as@amedeos-MacBook-Air.local'
grep -qF "$KEY" /root/.ssh/authorized_keys 2>/dev/null || echo "$KEY" >> /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
