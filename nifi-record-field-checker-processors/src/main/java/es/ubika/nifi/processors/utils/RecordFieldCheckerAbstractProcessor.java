package es.ubika.nifi.processors.utils;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public abstract class RecordFieldCheckerAbstractProcessor extends AbstractProcessor {

  static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
          .name("record-reader")
          .displayName("Record Reader")
          .description("Specifies the Controller Service to use for reading incoming data")
          .identifiesControllerService(RecordReaderFactory.class)
          .required(true)
          .build();

  static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
          .name("include-zero-record-flowfiles")
          .displayName("Include Zero Record FlowFiles")
          .description("When converting an incoming FlowFile, if the conversion results in no data, "
                  + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
          .expressionLanguageSupported(ExpressionLanguageScope.NONE)
          .allowableValues("true", "false")
          .defaultValue("true")
          .required(true)
          .build();

  public static final Relationship VALID = new Relationship.Builder()
          .name("valid")
          .description("The record meets the requirements.")
          .build();

  public static final Relationship INVALID = new Relationship.Builder()
          .name("invalid")
          .description("The record doesn't meet the requirements.")
          .build();

  static final Relationship REL_FAILURE = new Relationship.Builder()
          .name("failure")
          .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                  + "the unchanged FlowFile will be routed to this relationship")
          .build();


  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(VALID);
    relationships.add(INVALID);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(RECORD_READER);
    return properties;
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
    final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).isSet()? context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean():true;

    final Map<String, String> attributes = new HashMap<>();

    final FlowFile original = flowFile;
    final Map<String, String> originalAttributes = flowFile.getAttributes();
    final List<Boolean> records = new ArrayList<>();
    try {
      session.read(flowFile, new InputStreamCallback() {
        @Override
        public void process(final InputStream in) throws IOException {

          try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, getLogger())) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
              records.add(RecordFieldCheckerAbstractProcessor.this.process(record, original, context));
            }
          } catch (final SchemaNotFoundException e) {
            throw new ProcessException(e.getLocalizedMessage(), e);
          } catch (final MalformedRecordException e) {
            throw new ProcessException("Could not parse incoming data", e);
          }
        }
      });
    } catch (final Exception e) {
      getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
      session.transfer(flowFile, REL_FAILURE);
      return;
    }

    boolean isValid = false;

    for(Boolean bool: records) {
      isValid = isValid || bool;
    }

    if(isValid) {
      session.transfer(flowFile, VALID);
    } else {
      session.transfer(flowFile, INVALID);
    }
  }

  protected abstract boolean process(Record record, FlowFile flowFile, ProcessContext context);
}