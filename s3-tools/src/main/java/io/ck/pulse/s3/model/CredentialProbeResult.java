/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3.model;

public class CredentialProbeResult {
    private final CredentialSource source;
    private final int chainOrder;
    private final boolean selected;
    private final boolean available;
    private final boolean usable;
    private final String accessKeyIdMasked;
    private final String callerArn;
    private final String callerAccountId;
    private final String message;

    public CredentialProbeResult(
        CredentialSource source,
        int chainOrder,
        boolean selected,
        boolean available,
        boolean usable,
        String accessKeyIdMasked,
        String callerArn,
        String callerAccountId,
        String message
    ) {
        this.source = source;
        this.chainOrder = chainOrder;
        this.selected = selected;
        this.available = available;
        this.usable = usable;
        this.accessKeyIdMasked = accessKeyIdMasked;
        this.callerArn = callerArn;
        this.callerAccountId = callerAccountId;
        this.message = message;
    }

    public CredentialSource getSource() {
        return source;
    }

    public int getChainOrder() {
        return chainOrder;
    }

    public boolean isSelected() {
        return selected;
    }

    public boolean isAvailable() {
        return available;
    }

    public boolean isUsable() {
        return usable;
    }

    public String getAccessKeyIdMasked() {
        return accessKeyIdMasked;
    }

    public String getCallerArn() {
        return callerArn;
    }

    public String getCallerAccountId() {
        return callerAccountId;
    }

    public String getMessage() {
        return message;
    }
}
