# Issue #480 evidence

Owner: `ppzxc` (issue assignee; build/module owner)
Rules: ADR-0003, ADR-0006, ADR-0007; Java quality policy ARC-005, TOOL-001
Target SHA: `48c7bd1b2ad8f766fa86da38897d7b53a54be6b3`
Environment: JDK 25, Gradle 9.9.1, Maven 3.9.11, local `0.2.0` publication
Recorded: 2026-11-25 19:21 Asia/Seoul

| Check | Command | Result |
|---|---|---|
| Core architecture rules, including a controlled transport-dependency violation sample | `./gradlew :strand-core:test --tests '*CoreArchitectureTest' --no-daemon --console=plain` | PASS |
| Gradle and Maven consumers, classpath and module path | `deploy-test/run.sh 0.2.0` | PASS; four consumers resolved and ran the local publication |
| Gradle allowlist violation sample | `./gradlew -p deploy-test/gradle-classpath verifyResolvedDependencies -PstrandVersion=0.2.0 -I /tmp/strand-480/add-unapproved-dependency.gradle --no-daemon --console=plain` | FAIL as expected; rejected `com.google.code.gson:gson` |
| Maven allowlist violation sample | temporary Maven POM with `com.google.code.gson:gson:2.15.0`; `mvn validate -DstrandVersion=0.2.0` | FAIL as expected; Enforcer rejected Gson |

The architecture checks enforce the project-specific ADR boundaries. The API review found no design change needed. The dependency allowlist checks the resolved consumer runtime graph, including transitive modules.
