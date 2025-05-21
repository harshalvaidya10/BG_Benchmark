#!/bin/bash
# Script overview: configure an OpenSSH Server inside a Docker internal network environment with no security restrictions
# Requirements: run as root (no sudo needed)

# 1. Update package list and install OpenSSH Server and rsync
apt update && apt install -y openssh-server rsync unzip

# 2. Create the directory required by SSH
mkdir -p /var/run/sshd

# 3. Modify /etc/ssh/sshd_config:
#    — Allow root login
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
#    — Allow empty-password login
sed -i 's/#PermitEmptyPasswords no/PermitEmptyPasswords yes/' /etc/ssh/sshd_config
#    — Disable PAM authentication
sed -i 's/UsePAM yes/UsePAM no/' /etc/ssh/sshd_config

# If any of these directives is missing, append it:
grep -q "^PermitRootLogin" /etc/ssh/sshd_config || echo "PermitRootLogin yes" >> /etc/ssh/sshd_config
grep -q "^PermitEmptyPasswords" /etc/ssh/sshd_config || echo "PermitEmptyPasswords yes" >> /etc/ssh/sshd_config
grep -q "^UsePAM" /etc/ssh/sshd_config || echo "UsePAM no" >> /etc/ssh/sshd_config

# 4. Remove the root password (to allow empty-password login)
passwd -d root

# 5. Restart the SSH service
service ssh restart

echo "OpenSSH Server is now running. You can connect with: ssh root@<container_ip> (just press Enter at the password prompt)."
