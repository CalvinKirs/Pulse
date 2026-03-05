/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3.model;

public class PermissionCheckResult {
    private final CredentialSource source;
    private final PermissionOperation operation;
    private final Boolean allowed;
    private final String target;
    private final Integer statusCode;
    private final String errorCode;
    private final String message;

    public PermissionCheckResult(
        CredentialSource source,
        PermissionOperation operation,
        Boolean allowed,
        String target,
        Integer statusCode,
        String errorCode,
        String message
    ) {
        this.source = source;
        this.operation = operation;
        this.allowed = allowed;
        this.target = target;
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.message = message;
    }

    public PermissionOperation getOperation() {
        return operation;
    }

    public CredentialSource getSource() {
        return source;
    }

    public Boolean getAllowed() {
        return allowed;
    }

    public String getTarget() {
        return target;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getMessage() {
        return message;
    }
}
