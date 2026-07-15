# NEF emulator Helm Chart

## CAPIF integration

Set `capif.enabled=true` to enable the CAPIF onboarding flow (mirrors `CAPIF_ENABLED=true` in the docker-compose setup). This deploys an `nginx` reverse proxy that terminates TLS/mTLS using the certificates the backend generates during CAPIF onboarding.

The backend and nginx share those certificate files through a `hostPath` volume (`capif.hostPath`, default `/mnt/nef-capif-certs`), since there's no shared filesystem across nodes by default. **This means every pod that needs the certs (nginx and any backend replicas) must be scheduled on the same node.** Set `capif.nodeSelector` (e.g. `{"kubernetes.io/hostname": "<node>"}`) to pin them there — this matters most on multi-node clusters; on a single-node dev cluster it's a no-op.

Also set `capif.host`, `capif.registerHost`, `capif.httpsPort`, `capif.registerPort`, `capif.username` and `capif.password` to match your CAPIF core deployment.
