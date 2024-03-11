package org.opensearch.dataprepper.plugins.processor.model.rule;

import org.opensearch.dataprepper.plugins.processor.parser.RuleParser;
import org.opensearch.dataprepper.plugins.processor.parser.SigmaV1RuleParser;

import java.util.Map;
import java.util.function.Function;

public enum RuleSchema {
    SIGMA_V1(SigmaV1RuleParser::new);

    private final Function<Map<String, String>, RuleParser> parserConstructor;

    RuleSchema(final Function<Map<String, String>, RuleParser> parserConstructor) {
        this.parserConstructor = parserConstructor;
    }

    public Function<Map<String, String>, RuleParser> getParserConstructor() {
        return parserConstructor;
    }
}
