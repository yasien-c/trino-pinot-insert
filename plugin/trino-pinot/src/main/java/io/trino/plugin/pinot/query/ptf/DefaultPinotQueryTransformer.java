package io.trino.plugin.pinot.query.ptf;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DefaultPinotQueryTransformer
        implements PinotQueryTransformer
{
    @Override
    public String transform(PinotRelationHandle pinotRelationHandle) {
        requireNonNull(pinotRelationHandle, "pinotRelationHandle is null");
        checkState(pinotRelationHandle instanceof PinotQueryRelationHandle, "Unsupported pinotRelationHandle type '%s'", pinotRelationHandle.getClass());
        PinotQueryRelationHandle pinotQueryRelationHandle = (PinotQueryRelationHandle) pinotRelationHandle;

        return null;
    }
}
