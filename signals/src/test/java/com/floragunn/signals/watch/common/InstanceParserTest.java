package com.floragunn.signals.watch.common;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.fluent.collections.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class InstanceParserTest {

    public static final String PARAM_ONE = "param_one";
    public static final String PARAM_TWO = "param_two";
    public static final String PARAM_THREE = "param_three";
    public static final String PARAM_FOUR = "param_four";
    public static final String PARAM_FIVE = "param_five";
    public static final String ATTRIBUTE_ENABLED = "enabled";
    public static final String ATTRIBUTE_PARAMS = "params";
    public static final String ATTRIBUTE_INSTANCES = "instances";
    private ValidationErrors validationErrors;
    private InstanceParser instanceParser;

    @Before
    public void before() {
        this.validationErrors = new ValidationErrors();
        this.instanceParser = new InstanceParser(validationErrors);
    }

    @Test
    public void shouldCreateDisabledInstanceWithoutParameters() {
        DocNode node = DocNode.EMPTY;
        ValidatingDocNode validatingNode = new ValidatingDocNode(node, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances, notNullValue());
        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithTwoParameters() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_ONE, PARAM_TWO));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(2));
        assertThat(instances.getParams(), contains(PARAM_ONE, PARAM_TWO));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithOneParameters() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_ONE));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(1));
        assertThat(instances.getParams(), contains(PARAM_ONE));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithManyParameters() {
        ImmutableList<String> parameters = ImmutableList.of(PARAM_ONE, PARAM_THREE, PARAM_FOUR, PARAM_FIVE);
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, parameters);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(4));
        assertThat(instances.getParams(), contains(PARAM_ONE, PARAM_THREE, PARAM_FOUR, PARAM_FIVE));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithNoParameters() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldCreateEnabledInstancesWithEmptyParametersList() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, ImmutableList.empty());
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(true));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.hasErrors(), equalTo(false));
    }

    @Test
    public void shouldReportErrorWhenEnabledAttributeIsMissing() {
        DocNode instanceNode = DocNode.of(ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_FIVE));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances, notNullValue());
        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getErrors(), hasKey("instances.enabled"));
    }

    @Test
    public void shouldReportErrorWhenParametersAreNotPlacedInList() {
        ImmutableMap<String, String> invalidParameterStructure = ImmutableMap.of(PARAM_ONE, PARAM_TWO);
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, true, ATTRIBUTE_PARAMS, invalidParameterStructure);
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getErrors(), hasKey("instances.params"));
    }

    @Test
    public void shouldReportErrorWhenEnabledAttributeIsNotBoolean() {
        String invalidEnabledAttributeValue = "active";
        DocNode instanceNode = DocNode.of(ATTRIBUTE_ENABLED, invalidEnabledAttributeValue, ATTRIBUTE_PARAMS, ImmutableList.of(PARAM_ONE));
        DocNode watchNode = DocNode.of(ATTRIBUTE_INSTANCES, instanceNode);
        ValidatingDocNode validatingNode = new ValidatingDocNode(watchNode, validationErrors);

        Instances instances = instanceParser.parse(validatingNode);

        assertThat(instances.isEnabled(), equalTo(false));
        assertThat(instances.getParams(), hasSize(0));
        assertThat(validationErrors.size(), equalTo(1));
        assertThat(validationErrors.getErrors(), hasKey("instances.enabled"));
    }

    // TODO add test related to parameters' name validation

}