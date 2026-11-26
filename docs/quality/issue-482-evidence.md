# Issue 482 evidence

- Owner: `strand-api`, `strand-load`, build
- Rules: STYLE-009; ADR-0001 and DTO decision #474
- Code SHA: `af799e1`
- Java 8 DTOs: `strand-api/src/main/java/io/github/ppzxc/strand/api/RequestOptions.java`, `ResourceUsage.java`. Decision #474 approves their declaration-specific record exceptions.
- Java 25 DTO: private `Matrix.Cell` in `strand-load/src/main/java/io/github/ppzxc/strand/load/Matrix.java`; ArchUnit requires a record.
- Excluded by decision #474: `RequestHandle`, `ResultCompletion`, and mutable `Orchestrator.LoadGenOutcome`.
- Other API immutability: `Request` already copies input and returned payloads. `RequestResult.payload()` now returns a copy; a regression test failed before the fix and passed after it.
- Commands: `./gradlew :strand-api:test --tests '*DtoRecordTest' --tests '*RequestResultTest' :strand-load:test --tests '*MatrixDtoTest' --console=plain`; `./gradlew :strand-api:compileJava :strand-load:compileJava --rerun-tasks --console=plain`.
- Results: focused tests passed. Java 8 API compilation and Java 25 load compilation passed.
- Independent internal review: PASS on integration commit `897679e`; DTO scope matches decision #474.
- GitHub ruleset: unchanged; zero required approvals, per the solo-maintainer workflow. No external reviewer approval is claimed.
