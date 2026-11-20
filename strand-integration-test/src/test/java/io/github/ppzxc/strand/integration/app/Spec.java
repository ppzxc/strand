package io.github.ppzxc.strand.integration.app;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;

/** 시험이 고치는 한도의 사양. 기본은 넉넉하고 진행 공간만 필요한 최소치로 도출한다. */
final class Spec {
    long perConnection = 8;
    long owner = 8;
    long connections = 4;
    long preparing = -1;
    long ownerIncomingCount = -1;
    long ownerOutgoingCount = -1;
    long connectionHandlerTasks = -1;
    long ownerHandlerTasks = -1;
    long perConnectionObservations = 64;
    long ownerObservations = -1;
    long maxFrameSize = EndpointConfig.DEFAULT_MAX_FRAME_SIZE;
    long bodyBytes = 1 << 20;
    long ownerBodyBytes = 1 << 24;

    Spec perConnection(long value) {
        this.perConnection = value;
        return this;
    }

    Spec owner(long value) {
        this.owner = value;
        return this;
    }

    Spec connections(long value) {
        this.connections = value;
        return this;
    }

    Spec preparing(long value) {
        this.preparing = value;
        return this;
    }

    /** 소유 객체 전체의 송신 요청 수 한도를 직접 정한다(기본은 {@code owner * connections}). */
    Spec ownerOutgoingCount(long value) {
        this.ownerOutgoingCount = value;
        return this;
    }

    Spec ownerIncomingCount(long value) {
        this.ownerIncomingCount = value;
        return this;
    }

    Spec observations(long perConnection, long owner) {
        this.perConnectionObservations = perConnection;
        this.ownerObservations = owner;
        return this;
    }

    Spec bodyBytes(long perConnection, long owner) {
        this.bodyBytes = perConnection;
        this.ownerBodyBytes = owner;
        return this;
    }

    Spec connectionHandlerTasks(long value) {
        this.connectionHandlerTasks = value;
        return this;
    }

    ResourceLimits build(long[] min) {
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(perConnection, bodyBytes)
                        .incomingRequests(perConnection, bodyBytes)
                        .responseBytes(bodyBytes)
                        .handlerTasks(connectionHandlerTasks < 0 ? perConnection : connectionHandlerTasks)
                        .observations(perConnectionObservations)
                        .preparationBytes(1 << 16)
                        .progressBytes(min[0], min[1], min[2], min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(preparing < 0 ? connections : preparing)
                        .totalConnections(connections)
                        .preparationTasks(connections, connections, connections)
                        .outgoingRequests(
                                ownerOutgoingCount < 0 ? owner * connections : ownerOutgoingCount, ownerBodyBytes)
                        .incomingRequests(
                                ownerIncomingCount < 0 ? owner * connections : ownerIncomingCount, ownerBodyBytes)
                        .responseBytes(ownerBodyBytes)
                        .handlerTasks(ownerHandlerTasks < 0 ? owner * connections : ownerHandlerTasks)
                        .observations(ownerObservations < 0 ? 64 * connections : ownerObservations)
                        .preparationBytes(1 << 20)
                        .progressBytes(
                                min[0] * connections,
                                min[1] * connections,
                                min[2] * connections,
                                min[3] * connections,
                                min[4] * connections,
                                min[5] * connections)
                        .build())
                .build();
    }
}
