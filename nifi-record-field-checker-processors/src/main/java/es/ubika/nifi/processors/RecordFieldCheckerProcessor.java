/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.ubika.nifi.processors;

import es.ubika.nifi.processors.utils.RecordFieldCheckerAbstractProcessor;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record,fields,checker"})
@CapabilityDescription("Checks whether the specified fields exists or not in the incoming record depending on property value. Sends the FlowFile to the Valid relationship if all the rules are matched, otherwise it's sent to the Invalid relationship.")
public class RecordFieldCheckerProcessor extends RecordFieldCheckerAbstractProcessor {

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Specifies the name of the fields of the record to check. True value to validate that the field exists, false to validate that the field doesn't exist.")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    protected boolean process(Record record, FlowFile flowFile, ProcessContext processContext) {
        boolean out = true;
        for(PropertyDescriptor prop: processContext.getProperties().keySet()) {
            if (prop.isDynamic()) {
                boolean validator = processContext.getProperty(prop).asBoolean();
                out = out && (validator == containsField(prop.getName(), record));
            }
        }
        return out;
    }

    private boolean containsField(String fieldName, Record record) {
        boolean out = false;
        List<RecordField> fields = record.getSchema().getFields();
        for (RecordField field: fields) {
            out = out || fieldName.equals(field.getFieldName());
        }
        return out;
    }
}
