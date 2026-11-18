package io.github.ppzxc.strand.core.internal;

/** 유한 입력 처리의 결과: 소비한 바이트 수와, 수거 공간이 모자라 멈췄는지. 내부 계약이며 제품 공개 API가 아니다. */
public final class ReceiveProgress {
    private final int consumed;
    private final boolean collectionRequired;

    ReceiveProgress(int consumed, boolean collectionRequired) {
        this.consumed = consumed;
        this.collectionRequired = collectionRequired;
    }

    /** 이 호출에서 소비한 바이트 수. 다음 호출은 이 위치부터 이어 입력한다. */
    public int consumed() {
        return consumed;
    }

    /** 사건·출력을 수거해야 다음 단위를 처리할 수 있어 멈췄으면 참. */
    public boolean collectionRequired() {
        return collectionRequired;
    }
}
