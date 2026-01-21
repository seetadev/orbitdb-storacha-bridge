# Tests

This folder contains automated tests for OrbitDB Storacha Bridge. The suites
cover integration flows, CAR-based backups, network download behavior, and
edge-case handling.

## Integration Tests (`test/integration.test.js`)

Validates end-to-end backup and restore flows for OrbitDB databases using
Storacha. Highlights include:

- Full backup + restore with CID mappings (fast, deterministic restore).
- Mapping-independent restore from space (space scan + manifest discovery).
- Key-value and documents databases with DEL operations.
- Identity and access-controller preservation checks.

By default, tests use the **in-memory Storacha service**. To run against
production Storacha, set:

```bash
USE_PRODUCTION_STORACHA=1 STORACHA_KEY=... STORACHA_PROOF=... npm run test:integration
```

## Network Download Tests (`test/network-download.test.js`)

Exercises IPFS network download behavior and gateway fallback paths, including:

- `downloadBlockFromIPFSNetwork` using UnixFS DAG traversal.
- `downloadBlockFromStoracha` with network-first and gateway fallback.
- `restoreFromSpaceCAR` for CAR-based backups over network/gateway.

These tests also run **in-memory by default** (same switch as above).

## CAR Backup Tests (`test/backup-car.test.js`, `test/car-storage.test.js`)

Validates CAR-based backup/restore behavior and storage persistence. Includes
fallback/compat checks and file-level storage behaviors.

## Other Suites

- `test/access-control-integration.test.js`: UCAN access control flows.
- `test/ipns-restore.test.js`: IPNS-related restore scenarios.
- `test/p256-ucan-security.test.js`: P-256 UCAN security tests.
- `test/timestamped-backup.test.js`: timestamped CAR backup behavior.

## Running Tests

```bash
npm test
npm run test:integration
npm run test:car-backup
npm run test:network-download
```
