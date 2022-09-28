# jito-relayer
Transaction Relayer

# Setup

## Generate RSA keys:
One needs to generate RSA keys for JWT key generation and verification. To do that, use the following scripts:
```asm
OUTPUT_DIR=
openssl genrsa --out $OUTPUT_DIR/private.pem
openssl rsa --in $OUTPUT_DIR/private.pem --pubout --out $OUTPUT_DIR/public.pem
```
