/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class S3ConnectivityReport {
    private final String region;
    private final String endpoint;
    private final String bucket;
    private final String prefix;
    private final CredentialSource selectedCredentialSource;
    private final String selectedAccessKeyMasked;
    private final String selectedCallerArn;
    private final List<CredentialProbeResult> credentialProbeResults;
    private final List<PermissionCheckResult> permissionCheckResults;

    public S3ConnectivityReport(
        String region,
        String endpoint,
        String bucket,
        String prefix,
        CredentialSource selectedCredentialSource,
        String selectedAccessKeyMasked,
        String selectedCallerArn,
        List<CredentialProbeResult> credentialProbeResults,
        List<PermissionCheckResult> permissionCheckResults
    ) {
        this.region = region;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.prefix = prefix;
        this.selectedCredentialSource = selectedCredentialSource;
        this.selectedAccessKeyMasked = selectedAccessKeyMasked;
        this.selectedCallerArn = selectedCallerArn;
        this.credentialProbeResults = Collections.unmodifiableList(new ArrayList<>(credentialProbeResults));
        this.permissionCheckResults = Collections.unmodifiableList(new ArrayList<>(permissionCheckResults));
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public CredentialSource getSelectedCredentialSource() {
        return selectedCredentialSource;
    }

    public String getSelectedAccessKeyMasked() {
        return selectedAccessKeyMasked;
    }

    public String getSelectedCallerArn() {
        return selectedCallerArn;
    }

    public List<CredentialProbeResult> getCredentialProbeResults() {
        return credentialProbeResults;
    }

    public List<PermissionCheckResult> getPermissionCheckResults() {
        return permissionCheckResults;
    }
}
