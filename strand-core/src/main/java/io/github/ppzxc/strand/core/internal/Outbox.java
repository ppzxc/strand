package io.github.ppzxc.strand.core.internal;

import org.jspecify.annotations.Nullable;

final class Outbox {

    static final class Node {
        final OutboundMessage message;
        final boolean application;

        long waitId = LivenessTimers.NO_WAIT;

        @Nullable
        Object owner;

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

    private @Nullable Node lastControl;

    private int size;

    private long bytes;

    Node addApplication(OutboundMessage message) {
        Node node = new Node(message);
        linkAfter(tail, node);
        return node;
    }

    Node addControl(OutboundMessage message) {
        Node node = new Node(message);
        linkAfter(lastControl, node);
        lastControl = node;
        return node;
    }

    Node addAfter(Node anchor, OutboundMessage message) {
        Node node = new Node(message);
        linkAfter(anchor, node);
        return node;
    }

    void remove(Node node) {
        if (node.waitId != LivenessTimers.NO_WAIT) {
            throw new IllegalStateException("확인 대기가 붙은 노드는 거둘 수 없다: " + node.waitId);
        }
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
        bytes -= node.message.length();
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

    int size() {
        return size;
    }

    long bytes() {
        return bytes;
    }

    boolean isEmpty() {
        return size == 0;
    }

    void discard() {
        head = null;
        tail = null;
        lastControl = null;
        size = 0;
        bytes = 0;
    }

    @Nullable
    Node first() {
        return head;
    }

    void removeForWriting(Node node) {
        if (node == lastControl) {
            lastControl = node.prev;
        }
        node.waitId = LivenessTimers.NO_WAIT;
        remove(node);
    }

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
        bytes += node.message.length();
    }
}
