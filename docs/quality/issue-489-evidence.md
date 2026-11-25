# Issue 489 verification evidence

Owner: strand module maintainers
Rules: VER-005, VER-006, VER-009, VER-011, TST-004
Gate: Integration; Deep for resource and shutdown behavior

| Behavior | Specification and vector evidence | Executed path and assertion |
|---|---|---|
| Frame and WebSocket message boundaries | STRAND_SPECIFICATION §1.2, §3; `required-vectors.txt` B-11; `FrameLayoutTest` | `NettyWebSocketMessageTest`: text, multiple-frame, truncated-frame, fragmented-message, and frame-size cases assert close reason or returned body. |
| Duplicate and state transitions | STRAND_SPECIFICATION §4; `required-vectors.txt` B-6, B-13–18, B-29; `VectorConformanceTest` | `:strand-conformance:test` runs vectors through Core, NIO, and Netty-supported paths. |
| Timeout behavior | STRAND_SPECIFICATION §4; `required-vectors.txt` B-24; `AckWaitsTest` | `:strand-conformance:test` checks timeout boundaries and cancellation vectors. |
| WebSocket close mapping | STRAND_SPECIFICATION §1.2, §5; WebSocket close vectors; `NettyWebSocketCloseTest` | Real strand-netty WebSocket peer asserts close status and public `CloseNotice`. |
| Request result and resource recovery | STRAND_SPECIFICATION §5; `CompletionBoundaryTest`, `ResourceReleaseTest` | `ResourceReleaseTest` separately asserts pending request result, completed shutdown, zero resource counters, and terminated threads/sockets. |

No new protocol behavior was needed: these existing integration and conformance tests cover the requested seams. No randomized test or optional PIT/property tool was added; deterministic boundary vectors and integration cases provide the required evidence.

Command: `./gradlew :strand-conformance:test :strand-netty:test --tests '*NettyWebSocketMessageTest' --tests '*NettyWebSocketCloseTest' :strand-integration-test:test --tests '*ResourceReleaseTest' --console=plain`

Result: PASS on `b16ec203222e290170adba3610f15e1e6d461135` (25 November 2026, local JDK 25).
