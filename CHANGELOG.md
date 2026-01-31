# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-31

### Added
- Initial release
- WebSocket client with auto-reconnection + xponential backoff + jitter
- Multi-endpoint failover
- Event processor with ABI decoding (OrderFilled events)
- LRU deduplication cache
- CLI tools: `ctf-stream stream`, `ping`, `version`
