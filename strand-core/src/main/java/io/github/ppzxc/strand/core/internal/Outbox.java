package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * 상대에게 보낼, 아직 쓰기를 시작하지 않은 메시지의 쓰기 대기열(규격 3.6절). 꺼낸 메시지가 쓰기를 시작한 것이므로 대기열에 남은 메시지는 모두
 * 쓰기 전이다.
 *
 * <p>순서 규칙은 이렇다.
 *
 * <ul>
 *   <li>애플리케이션 프레임끼리는 넣은 순서 그대로다({@link #addApplication}).
 *   <li>제어 프레임은 쓰기 전 애플리케이션 프레임 앞에 둔다. 제어 프레임끼리도 넣은 순서 그대로다({@link #addControl}).
 *   <li>CLOSE는 쓰기 전 요청 가운데 마지막 것 바로 뒤에 둔다({@link #addAfter}). 앞서 보내면 그 요청이 CLOSE 뒤에 온 새 요청이 되어 상대가
 *       버리기 때문이다(규격 3.5절).
 * </ul>
 *
 * <p>이중 연결 리스트라 넣기, 중간 노드 지우기가 모두 O(1)이다. 앞쪽의 비애플리케이션 노드가 이어진 구간(제어 구간)의 마지막 노드를 {@link
 * #lastControl}로 따로 들고 있어, 제어 프레임을 넣을 때 리스트를 훑지 않는다. 이 구간이 늘어나는 때는 첫 애플리케이션 노드를 지울 때와 비애플리케이션
 * 노드를 맨 뒤에 붙일 때뿐이다.
 */
final class Outbox {

    /** 쓰기 대기열의 노드. 메시지 하나와 그 메시지가 시작시킬 확인 대기를 이어 둔다. */
    static final class Node {
        final OutboundMessage message;
        final boolean application;

        @Nullable
        AckWait wait;

        @Nullable
        Node prev;

        @Nullable
        Node next;

        Node(OutboundMessage message) {
            this.message = message;
            this.application = message.isApplication();
        }
    }

    private @Nullable Node head;
    private @Nullable Node tail;
    /** 앞쪽에서 비애플리케이션 노드가 이어진 구간의 마지막 노드. 그런 노드가 없으면 null이다. */
    private @Nullable Node lastControl;

    private int size;

    /** 애플리케이션 프레임을 맨 뒤에 붙인다. */
    Node addApplication(OutboundMessage message) {
        Node node = new Node(message);
        linkAfter(tail, node);
        return node;
    }

    /** 제어 프레임을 제어 구간의 끝, 곧 쓰기 전 애플리케이션 프레임 앞에 넣는다. */
    Node addControl(OutboundMessage message) {
        Node node = new Node(message);
        linkAfter(lastControl, node);
        lastControl = node;
        return node;
    }

    /** {@code anchor} 바로 뒤에 넣는다. {@code anchor}는 대기열에 있는 애플리케이션 노드여야 한다. */
    Node addAfter(Node anchor, OutboundMessage message) {
        Node node = new Node(message);
        linkAfter(anchor, node);
        return node;
    }

    /** 메시지를 맨 뒤에 붙인다. 앞쪽 비애플리케이션 구간이 끝까지 이어져 있었다면 새 노드까지 그 구간이다. */
    Node addLast(OutboundMessage message) {
        Node node = new Node(message);
        boolean extendsControl = !node.application && tail == lastControl;
        linkAfter(tail, node);
        if (extendsControl) {
            lastControl = node;
        }
        return node;
    }

    /**
     * 애플리케이션 노드를 대기열에서 거둔다. 첫 애플리케이션 노드였다면 그 뒤에 이어진 비애플리케이션 노드(앞서 보내지 못해 남은 CLOSE 등)가
     * 제어 구간에 들어온다.
     */
    void remove(Node node) {
        Node before = node.prev;
        Node after = node.next;
        if (before == null) {
            head = after;
        } else {
            before.next = after;
        }
        if (after == null) {
            tail = before;
        } else {
            after.prev = before;
        }
        node.prev = null;
        node.next = null;
        size--;
        if (before == lastControl) {
            Node cursor = before;
            Node following = cursor == null ? head : cursor.next;
            while (following != null && !following.application) {
                cursor = following;
                following = following.next;
            }
            lastControl = cursor;
        }
    }

    boolean isEmpty() {
        return size == 0;
    }

    /**
     * 쌓인 메시지를 순서대로 모두 꺼내 대기열을 비운다. 꺼낸 메시지의 확인 대기는 이때부터 센다(규격 3.3절).
     *
     * @param ackDeadline 확인 대기의 기한. 아직 시작하지 않은 대기가 이 기한으로 시작한다
     */
    List<OutboundMessage> drain(long ackDeadline) {
        List<OutboundMessage> drained = new ArrayList<>(size);
        for (Node node = head; node != null; node = node.next) {
            drained.add(node.message);
            AckWait wait = node.wait;
            if (wait != null && !wait.started) {
                wait.start(ackDeadline);
            }
        }
        head = null;
        tail = null;
        lastControl = null;
        size = 0;
        return drained;
    }

    /** {@code anchor} 바로 뒤에 잇는다. {@code anchor}가 null이면 맨 앞이다. */
    private void linkAfter(@Nullable Node anchor, Node node) {
        Node after = anchor == null ? head : anchor.next;
        node.prev = anchor;
        node.next = after;
        if (anchor == null) {
            head = node;
        } else {
            anchor.next = node;
        }
        if (after == null) {
            tail = node;
        } else {
            after.prev = node;
        }
        size++;
    }
}
