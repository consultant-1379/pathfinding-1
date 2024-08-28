package com.ericsson.oss.eniq.techpack.service.interfaces;

import com.ericsson.oss.eniq.techpack.model.Versioning;

public interface VersioningService {
    Versioning getTargetVersioning(String requiredTechPackName);
}
