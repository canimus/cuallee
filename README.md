# cuallee
Meaning `good` in Aztec (Nahuatl)


Create certificates
```
openssl req -x509 -sha256 -nodes -days 365 -newkey rsa:4096 -keyout private.key -out certificate.crt
```

Run
```
hypercorn --certfile .\certificate.crt --keyfile .\private.key --reload  app:app
```