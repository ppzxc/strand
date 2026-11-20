package io.github.ppzxc.strand.core.internal;

/** 전송 모듈이 사건과 출력을 아직 수거하지 못한 채 core에 남겨 둘 수 있는 최대 개수. 내부 계약이며 제품 공개 API가 아니다. */
public final class CollectionSpace {
    /** 한도가 없다. */
    public static final CollectionSpace UNLIMITED = new CollectionSpace(Integer.MAX_VALUE, Integer.MAX_VALUE);

    private final int events;
    private final int outputs;

    private CollectionSpace(int events, int outputs) {
        this.events = events;
        this.outputs = outputs;
    }

    /**
     * 수거 공간을 만든다.
     *
     * @param events 수거 전에 쌓아 둘 수 있는 사건 수
     * @param outputs 수거 전에 쌓아 둘 수 있는 출력 수
     * @return 공간
     * @throws IllegalArgumentException 한도가 0 이하일 때
     */
    public static CollectionSpace of(int events, int outputs) {
        if (events < 1 || outputs < 1) {
            throw new IllegalArgumentException("수거 공간은 양수여야 한다");
        }
        return new CollectionSpace(events, outputs);
    }

    int events() {
        return events;
    }

    int outputs() {
        return outputs;
    }
}
