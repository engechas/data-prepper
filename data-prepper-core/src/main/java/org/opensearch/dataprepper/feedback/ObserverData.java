package org.opensearch.dataprepper.feedback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObserverData {
    final Map<Long, List<Double>> collectedData = new HashMap<>();

    public void addDataPoint(final Long configurationValue, final double dataPoint) {
        if (!collectedData.containsKey(configurationValue)) {
            collectedData.put(configurationValue, new ArrayList<>());
        }

        collectedData.get(configurationValue).add(dataPoint);
    }

    public List<Double> getDataForConfiguration(final Long configurationValue) {
        return collectedData.get(configurationValue);
    }
}
