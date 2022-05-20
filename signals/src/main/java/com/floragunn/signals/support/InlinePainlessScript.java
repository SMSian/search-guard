package com.floragunn.signals.support;

import java.io.IOException;
import java.util.Collections;

import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptType;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class InlinePainlessScript<Factory> implements ToXContentFragment {
    private final ScriptContext<Factory> scriptContext;
    private final String source;
    private Factory scriptFactory;

    public InlinePainlessScript(ScriptContext<Factory> scriptContext, String source) {
        this.source = source;
        this.scriptContext = scriptContext;
    }

    public void compile(WatchInitializationService watchInitializationService, ValidationErrors validationErrors) {
        this.scriptFactory = watchInitializationService.compile(null, new Script(ScriptType.INLINE, "painless", source, Collections.emptyMap()),
                scriptContext, validationErrors);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.value(source);

        return builder;
    }

    public Factory getScriptFactory() {
        return scriptFactory;
    }

    public static <Factory> InlinePainlessScript<Factory> parse(String string, ScriptContext<Factory> scriptContext,
            WatchInitializationService watchInitializationService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        InlinePainlessScript<Factory> result = new InlinePainlessScript<Factory>(scriptContext, string);

        result.compile(watchInitializationService, validationErrors);
        validationErrors.throwExceptionForPresentErrors();
        return result;
    }

}
