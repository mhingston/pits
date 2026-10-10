<p align="center">
  <img src="assets/pits-logo.png" alt="Pi In The Sky (PITS) logo" width="240" />
</p>

# Pi In The Sky (PITS)

Experimental, remote-first durable coding agents built with Pi Durable and Cloudflare.

Development begins with **S0: an execution and recovery feasibility spike**. The initial experiment is intentionally headless and focuses on command deduplication, interruption classification, and workspace checkpoints rather than a UI.

See [the S0 runbook](docs/S0.md) for the implementation, recovery protocol, and validation status. Do not deploy this experiment with real secrets or untrusted repositories until its isolation and recovery assumptions have been tested.

The S0 runtime is pinned to `@cloudflare/sandbox` 1.0.0, `agents` 0.28.0,
`@earendil-works/pi-durable` 1.0.0, and Wrangler 4.149.0. Run local checks with
`npm ci && npx wrangler types && npm run typecheck && npm test`. Live destructive
recovery tests are separate and must target a disposable deployment; see the
runbook for setup and `PITS_ITERATIONS=10 npm run test:integration`.
