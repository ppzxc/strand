# Strand API contracts

`strand-api` contains transport-independent values used by the NIO and Netty public APIs. The wire protocol is defined by the [Strand specification](../docs/specification/STRAND_SPECIFICATION.md); this page records the application-facing contracts of the shared values.

## Requests and retained results

`Request.of(type, payload)` accepts application frame types (`0x100` through `0xffff`) and copies the payload. `payload()` returns a copy; `payloadLength()` reports its byte length without making another copy. `extensions(value)` returns a request using the supplied validated wire extensions. Local request deadlines are set with `RequestOptions.deadlineAfter(duration)`: negative durations are rejected, zero expires immediately, and a deadline does not set a wire timeout extension.

`RequestHandle.result()` retains one `RequestResult`. The first response, cancellation, deadline expiration, or connection failure processed by the transport fixes the result; later events do not replace it. `cancel()` requests cancellation but does not prove that cancellation won. Read the retained result to determine its effect. `Result.isDone()` and `resultNow()` provide non-blocking access; `resultNow()` throws `IllegalStateException` before completion.

`RequestResult` retains status, certainty, payload, and an optional exhausted resource. Its payload is application-owned. `Certainty` distinguishes a received response, a request known not to have been processed, and an unknown processing outcome. `ResponseAcceptance` describes the local response admission decision (`ACCEPTED`, `ALREADY_RESPONDED`, `REQUEST_ENDED`, or `RESOURCE_EXHAUSTED`); acceptance does not mean the peer received the response.

## Observations

`Result.observe` and `toCompletionStage` schedule observer work on the selected executor. An `Observation` reports only that work's completion or failure; observer failure does not alter the retained result or prevent other observers from running. `failureNow()` is available after `isDone()` becomes true and throws before then. Observation registration uses finite per-connection and per-owner allowances. Exhaustion is reported as `ResourceExhaustedException` for `OBSERVATIONS`.

`ResultCompletion` is for transport implementations. Its optional dedicated allowance reserves one observation slot for that result even when general observation allowances are exhausted; further observations use the general allowances. Its `listen` method runs on the result-completing thread, reserves the same bounded capacity, and is intended for transport-side coordination.

## Resource limits and shutdown values

`ResourceLimits` combines per-connection and per-owner budgets. Transport builders determine the minimum progress space required by the selected frame and TLS configuration, then call `requireProgress` to check both scopes. Frame assembly, control output, and result-event progress require positive space. TLS plaintext, ciphertext, and control space may be zero for a plain connection. The API module checks supplied minima; it does not derive transport-specific minima. `MinimumProgressSpace` carries those per-connection minima in bytes.

`ResourceUsage` is a snapshot of one resource's configured limit, current reservation, high-water reservation, and rejection count. A zero limit is valid only for TLS progress resources. `ShutdownResult` records whether shutdown completed or reached its deadline and snapshots remaining resources at that deadline.

`CloseNotice` retains whether a connection ended through a normal CLOSE, an error CLOSE, or a transport disconnect, along with the sender where applicable and a local failure cause where available. `Status` contains the wire status values 0 through 16 defined by the specification.
