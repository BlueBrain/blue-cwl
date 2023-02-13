#!/bin/bash

if [[ -z $VIRTUAL_ENV ]]; then
    echo "Need to be in virtualenv"
    exit 1
fi

CERT_PATH=`$VIRTUAL_ENV/bin/python3 -c "import certifi; print(certifi.where())"`

if [[ $? != 0 ]]; then
    echo "certifi missing?"
    exit 1
fi

if grep MIIBgTCCASegAwIBAgIQY $CERT_PATH >& /dev/null; then
    echo "certificate already installed?"
    exit 0
fi


# from https://bbpteam.epfl.ch/project/spaces/display/SDKB/Install+Root+Certificate+of+BBP+Internal+Certificate+Authority
cat << EOF >> $CERT_PATH
-----BEGIN CERTIFICATE-----
MIIBgTCCASegAwIBAgIQY++tFt5cRH9DpVKTR2VDIjAKBggqhkjOPQQDAjAfMR0w
GwYDVQQDExRCQlAgUm9vdCBDZXJ0aWZpY2F0ZTAeFw0yMTEyMDYxMzExMjlaFw0z
MTEyMDQxMzExMjlaMB8xHTAbBgNVBAMTFEJCUCBSb290IENlcnRpZmljYXRlMFkw
EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+6UOij4awGu/WoLp00KSqofNpfWW9gxF
SmWCNSSY4i5YG6kpMu2trZyKcPXsWnTyuVlLNM5ZEzpOlXxXFO/EuqNFMEMwDgYD
VR0PAQH/BAQDAgEGMBIGA1UdEwEB/wQIMAYBAf8CAQEwHQYDVR0OBBYEFLV6rXWM
3ekB6rIvf7DqQypk7WEnMAoGCCqGSM49BAMCA0gAMEUCIQDmkrWWAWXnvzFE9QEy
w/Nmaww2ZUdmlW1joJX1zHSmGAIgMj7V63Rtskh84SdKKU+u+T0T/mxLGgCdl1en
Vrvv3OA=
-----END CERTIFICATE-----
EOF
