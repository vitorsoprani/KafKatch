#!/bin/bash

INTERFACE=wlxe894f6281fb9
CON_NAME=Hotspot
AP_SSID=test-ap
AP_PSK=12345678

# WIFI SETUP
echo "Setting up network..."
MAIN_IF=$(ip route get 8.8.8.8 | awk -- '{printf $5}')
echo "Main network interface with internet detected: $MAIN_IF"

# ativa o roteamento ipv4
sudo sysctl -w net.ipv4.ip_forward=1
# bloqueia o trafego ipv6
sudo sysctl -w net.ipv6.conf.$INTERFACE.disable_ipv6=1

sudo nmcli con delete "$CON_NAME" 2>/dev/null
sudo nmcli con add type wifi ifname "$INTERFACE" con-name "$CON_NAME" autoconnect no ssid "$AP_SSID"
sudo nmcli con modify "$CON_NAME" \
    802-11-wireless.mode ap \
    802-11-wireless.band bg \
    802-11-wireless.channel 6 \
    802-11-wireless-security.key-mgmt wpa-psk \
    802-11-wireless-security.proto rsn \
    802-11-wireless-security.pairwise ccmp \
    802-11-wireless-security.group ccmp \
    802-11-wireless-security.psk "$AP_PSK" \
    ipv4.method shared \
    ipv6.method disabled
sudo nmcli con up "$CON_NAME"

# Limpa regras antigas de encaminhamento para evitar conflitos no testbed
sudo iptables -t nat -F POSTROUTING
sudo iptables -F FORWARD

# Habilita o NAT (Masquerade) na interface que tem internet
sudo iptables -t nat -A POSTROUTING -o "$MAIN_IF" -j MASQUERADE

# Permite o tráfego saindo da interface do Hotspot para a Internet
sudo iptables -A FORWARD -i "$INTERFACE" -o "$MAIN_IF" -j ACCEPT

# Permite o tráfego de volta (da Internet para o Hotspot) para conexões já estabelecidas
sudo iptables -A FORWARD -i "$MAIN_IF" -o "$INTERFACE" -m state --state RELATED,ESTABLISHED -j ACCEPT
