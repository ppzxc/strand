# Issue 485 evidence

- Owner: `strand-nio`, `strand-netty`, and `build-logic`
- Rules: `LOG-001` fixes the WARNING message to request type only; `LOG-002` requires a sanitized throwable that retains stack and cause types without messages.
- Scope: the two `ConnectionEngine#diagnoseHandlerFailure` declarations.
- Approval: internal independent review (`Codex independent review`), recorded 2026-11-25; both policy exceptions expire for review on 2027-02-23.
- Code SHA: `16830b79e9346ffda159ef4004e0e007387b9ec6`
- Commands: `./gradlew :build-logic:test --tests '*HandlerFailureLoggingRuleTest' :strand-nio:test --tests '*NioStrandUsageTest.handlerFailureLogKeepsDiagnosticContextWithoutSensitiveValues' :strand-netty:test --tests '*NettyStrandUsageTest.handlerFailureLogKeepsDiagnosticContextWithoutSensitiveValues' checkPolicyExceptions --console=plain`
- Results: PMD rule tests, rendered-log tests for NIO and Netty, and `checkPolicyExceptions` passed. The PMD tests reject remote address, payload, original throwable, appended exception messages, and request serialization while accepting the exact approved logger shape. Rendered-log tests verify WARNING level, request type, sanitized stack/cause, and absence of the address, body, and exception messages.
- Baseline: the existing Netty `EmptyCatch` suppression remains a legacy baseline finding. Its declaration moved from line 1275 to line 1285 after the method annotations were added; it received no new PolicyException or approval. The existing NIO class-level `EmptyCatch` suppression is unchanged.
