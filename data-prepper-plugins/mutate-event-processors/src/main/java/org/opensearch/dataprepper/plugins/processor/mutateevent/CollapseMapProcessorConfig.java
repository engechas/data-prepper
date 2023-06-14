package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class CollapseMapProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("source")
    private String source;

    @NotEmpty
    @NotNull
    @JsonProperty("target")
    private String target;

    @NotEmpty
    @NotNull
    @JsonProperty("collapsed_key")
    private String collapsedKey;

    @NotEmpty
    @NotNull
    @JsonProperty("collapsed_value_pattern")
    private String collapsedValuePattern;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getCollapsedKey() {
        return collapsedKey;
    }

    public String getCollapsedValuePattern() {
        return collapsedValuePattern;
    }
}
