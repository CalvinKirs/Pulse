/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3.model;

public enum CredentialSource {
    STATIC,
    SYSTEM_PROPERTIES,
    ENVIRONMENT,
    WEB_IDENTITY,
    PROFILE,
    CONTAINER,
    INSTANCE_PROFILE
}
