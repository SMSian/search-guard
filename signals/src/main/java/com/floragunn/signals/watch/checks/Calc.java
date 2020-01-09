package com.floragunn.signals.watch.checks;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptType;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.ValidatingJsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.JacksonTools;
import com.floragunn.signals.execution.CheckExecutionException;
import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.script.SignalsScript;
import com.floragunn.signals.watch.init.WatchInitializationService;

public class Calc extends Check {

    private String id;
    private String source;
    private String lang;
    private Map<String, Object> params;
    private Script script;
    private CalcScript.Factory scriptFactory;

    public Calc(String name, String id, String source, String lang, Map<String, Object> params) {
        super(name);
        this.id = id;
        this.source = source;
        this.lang = lang;
        this.params = params != null ? params : Collections.emptyMap();

        if (id != null) {
            script = new Script(ScriptType.STORED, null, id, this.params);
        } else {
            script = new Script(ScriptType.INLINE, lang != null ? lang : "painless", source, this.params);
        }
    }

    static Calc create(WatchInitializationService watchInitService, ObjectNode jsonObject) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonObject, validationErrors);

        vJsonNode.used("type");

        String id = vJsonNode.string("script_id");
        String name = vJsonNode.string("name");
        String lang = vJsonNode.string("lang");
        String source = vJsonNode.string("source");

        Map<String, Object> params = JacksonTools.toMap(vJsonNode.get("params"));

        vJsonNode.validateUnusedAttributes();
        
        validationErrors.throwExceptionForPresentErrors();

        Calc result = new Calc(name, id, source, lang, params);

        result.compileScripts(watchInitService);

        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (name != null) {
            builder.field("name", name);
        }

        builder.field("type", "calc");

        if (id != null) {
            builder.field("script_id", id);
        }

        if (source != null) {
            builder.field("source", source);
        }

        if (lang != null) {
            builder.field("lang", lang);
        }

        // TODO params
        // builder.endObject();

        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public String getSource() {
        return source;
    }

    public String getLang() {
        return lang;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public boolean execute(WatchExecutionContext ctx) throws CheckExecutionException {
        try {
            CalcScript transformScript = scriptFactory.newInstance(script.getParams(), ctx);
            transformScript.execute();

            return true;
        } catch (ScriptException e) {
            throw new CheckExecutionException(this, "Script Execution Error", e);
        }
    }

    public void compileScripts(WatchInitializationService watchInitService) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        this.scriptFactory = watchInitService.compile(this.id != null ? "script_id" : "source", script, CalcScript.CONTEXT, validationErrors);

        validationErrors.throwExceptionForPresentErrors();
    }

    public static abstract class CalcScript extends SignalsScript {

        public static final String[] PARAMETERS = {};

        public CalcScript(Map<String, Object> params, WatchExecutionContext watchRuntimeContext) {
            super(params, watchRuntimeContext);
        }

        public abstract Object execute();

        public static interface Factory {
            CalcScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
        }

        public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("signals_calc", Factory.class);

    }
}