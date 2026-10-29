// 두 Gradle 소비자가 공유하는 저장소 설정. 입력은 -PrepoUrl(기본 mavenLocal)과 환경 변수 STRAND_REPO_TOKEN(선택)이다.
// STRAND_REPO_TOKEN이 있으면 저장소 요청에 `Authorization: Bearer <토큰>` 헤더를 붙인다.
val repoUrl = providers.gradleProperty("repoUrl").orElse("mavenLocal").get()
val repoToken = providers.environmentVariable("STRAND_REPO_TOKEN").orNull

repositories {
    if (repoUrl == "mavenLocal") {
        mavenLocal()
    } else {
        maven {
            url = uri(repoUrl)
            if (!repoToken.isNullOrEmpty()) {
                credentials(HttpHeaderCredentials::class) {
                    name = "Authorization"
                    value = "Bearer $repoToken"
                }
                authentication {
                    create<HttpHeaderAuthentication>("header")
                }
            }
        }
    }
    // 이행 의존성(JSpecify 등)은 항상 Central에서 받는다.
    mavenCentral()
}
