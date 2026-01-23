# Changes

## 0.4.3 (2026-01-23)

### Added
- IPFS network restore with gateway fallback and new network download tests.
- In-memory Storacha test mode with local gateway support and improved integration test harness.
- CAR backup documentation plus expanded test and example docs.
- New E2E coverage for Svelte examples.

### Changed
- Logging migrated to `@libp2p/logger` for structured output.
- Svelte example apps updated (UI polish, configs, and test setup).
- CI now runs Svelte E2E jobs sequentially and pins Playwright to IPv4 loopback.

### Fixed
- Integration test stability (sequencing, retries, and cleanup improvements).
- Linting and dependency fixes across the repo.

### Removed
- `examples/svelte/ucan-delegation` moved out of mainline (now on a separate branch).
