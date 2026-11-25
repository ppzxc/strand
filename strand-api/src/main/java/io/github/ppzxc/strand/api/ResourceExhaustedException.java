package io.github.ppzxc.strand.api;

import java.util.Objects;

public final class ResourceExhaustedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Resource resource;

    public ResourceExhaustedException(Resource resource) {
        super("resource exhausted: " + Objects.requireNonNull(resource));
        this.resource = resource;
    }

    public Resource resource() {
        return resource;
    }
}
