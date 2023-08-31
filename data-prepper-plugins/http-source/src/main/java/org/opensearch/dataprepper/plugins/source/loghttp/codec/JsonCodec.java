/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JsonCodec parses the json array format HTTP data into List&lt;{@link String}&gt;.
 * TODO: replace output List&lt;String&gt; with List&lt;InternalModel&gt; type
 * <p>
 */
public class JsonCodec implements Codec<List<String>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_MAP_TYPE_REFERENCE =
            new TypeReference<List<Map<String, Object>>>() {};

    @Override
    public List<String> parse(byte[] bytes) throws IOException {
        List<String> jsonList = new ArrayList<>();
        final List<Map<String, Object>> logList = mapper.readValue(bytes,
                LIST_OF_MAP_TYPE_REFERENCE);
        for (final Map<String, Object> log: logList) {
            final String recordString = mapper.writeValueAsString(log);
            jsonList.add(recordString);
        }

        return jsonList;
    }
}
