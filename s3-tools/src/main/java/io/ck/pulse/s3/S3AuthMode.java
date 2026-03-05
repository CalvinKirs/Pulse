/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3;

public enum S3AuthMode {
    AUTO,
    STATIC,
    SYSTEM_PROPERTIES,
    ENVIRONMENT,
    WEB_IDENTITY,
    PROFILE,
    CONTAINER,
    INSTANCE_PROFILE;

    public static S3AuthMode fromString(String value) {
        if (value == null || value.trim().isEmpty()) {
            return AUTO;
        }

        String normalized = value.trim().toLowerCase().replace("-", "").replace("_", "");
        if ("auto".equals(normalized)) {
            return AUTO;
        }
        if ("static".equals(normalized)) {
            return STATIC;
        }
        if ("system".equals(normalized) || "systemproperties".equals(normalized)) {
            return SYSTEM_PROPERTIES;
        }
        if ("env".equals(normalized) || "environment".equals(normalized)) {
            return ENVIRONMENT;
        }
        if ("webidentity".equals(normalized) || "webidentify".equals(normalized)) {
            return WEB_IDENTITY;
        }
        if ("profile".equals(normalized)) {
            return PROFILE;
        }
        if ("container".equals(normalized) || "ecs".equals(normalized)) {
            return CONTAINER;
        }
        if ("instanceprofile".equals(normalized) || "ec2".equals(normalized)) {
            return INSTANCE_PROFILE;
        }
        throw new IllegalArgumentException("Unsupported authMode: " + value);
    }
}
