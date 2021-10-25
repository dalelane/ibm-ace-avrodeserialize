package uk.co.dalelane.kafka;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbPolicy;
import com.ibm.broker.plugin.MbUserException;

/**
 * This is a Java compute node implementation for running in
 *  IBM App Connect Enterprise, that will deserialize messages retrieved
 *  from Apache Kafka using a KafkaConsumer node.
 *
 * It is for consuming from Kafka topics which contain messages serialized
 *  using Apache Avro schemas from an Apicurio schema registry.
 *
 * It is dependent on required configuration parameters from the
 *  {kafka}:schemaregistry policy.
 *
 * It uses two output terminals:
 *  - "out" - JSON objects with deserialized Kafka messages are propogated
 *             to this terminal
 *  - "alt" - binary data with Kafka messages that could not be
 *             deserialized are propogated to this terminal
 *
 *
 * @author Dale Lane <email@dalelane.co.uk>
 */
public class AvroDeserialize extends MbJavaComputeNode {

    // ------------------------------------------------------------------------
    //  CONFIGURATION
    // ------------------------------------------------------------------------

    // This node has a dependency on a policy to provide the details of
    //   the schema registry to use.
    //
    // The policy should look like this:
    //
    //    <?xml version="1.0" encoding="UTF-8"?>
    //    <policies>
    //      <policy policyType="UserDefined" policyName="schemaregistry" policyTemplate="UserDefined">
    //        <schema.registry.url>https://your-schema-registry-host</schema.registry.url>
    //        <schema.registry.username>your-schema-registry-username</schema.registry.username>
    //        <schema.registry.password>your-schema-registry-password</schema.registry.password>
    //        <schema.registry.encoding>binary/json</schema.registry.encoding>
    //      </policy>
    //    </policies>
    //

    /** Key for retrieving the URL for a schema registry from the {kafka}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_REGISTRY_URL      = "schema.registry.url";
    /** Key for retrieving the username for schema registry API calls from the {kafka}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_REGISTRY_USERNAME = "schema.registry.username";
    /** Key for retrieving the password for schema registry API calls from the {kafka}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_REGISTRY_PASSWORD = "schema.registry.password";
    /** Key for retrieving the binary/json encoding setting from the {kafka}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_ENCODING          = "schema.registry.encoding";


    // values retrieved from the policy

    /**
     * Authorization HTTP header to use in API requests to the schema registry.
     */
    private static String SCHEMA_REGISTRY_AUTH_HEADER = "";

    /**
     * The decoding approach to use when deserializing Kafka message data.
     */
    private static AvroEncodingApproach ENCODING_APPROACH = null;




    // ------------------------------------------------------------------------
    //  CONSTANTS
    // ------------------------------------------------------------------------

    // expected message data:
    //   byte  0   : should contain 0
    //   bytes 1-9 : should contain the schema id as an 8-byte long
    //   bytes 10- : should contain the serialized message data

    /**
     * The number of bytes used in Kafka messages to store the schema ID.
     *
     * This assumes that Kafka messages were produced with a schema ID
     * handler set to io.apicurio.registry.serde.DefaultIdHandler which uses
     * an eight-byte long to encode schema IDs
     *
     * cf. https://www.apicur.io/registry/docs/apicurio-registry/2.1.0.Final/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-types-serde-registry
     */
    private static final int SCHEMA_ID_BYTES_LEN = Long.BYTES;

    // The schema ID handler io.apicurio.registry.serde.Legacy4ByteIdHandler
    //  uses a four-byte integer to encode schema IDs, so if processing
    //  events using that, the following constant should be used instead.
    //
    // private static final int SCHEMA_ID_BYTES_LEN = Integer.BYTES;

    /** Index into the message bytes for the location of the magic byte. */
    private static final int BYTES_IDX_MAGICBYTE = 0;
    /** Index into the message bytes for the start of the schema id. */
    private static final int BYTES_IDX_SCHEMAID  = BYTES_IDX_MAGICBYTE + 1;
    /** Index into the message bytes for the start of the serialized mesage contents. */
    private static final int BYTES_IDX_MSGDATA   = BYTES_IDX_SCHEMAID + SCHEMA_ID_BYTES_LEN;




    // ------------------------------------------------------------------------
    //  CACHES
    // ------------------------------------------------------------------------

    // The slowest part of deserializing the Kafka message data is retrieving
    //  the Avro schema from the schema registry.
    // To mitigate this, the following maps keep a copy of schemas in memory
    //  so that they only need to be retrieved the first time they are needed
    //  after the compute node is started.
    //
    // Notes:
    //  1) For nodes that need to handle messages with a large variety of
    //      schemas, you may want to replace these with a least-recently-used
    //      map implementation to reduce the number of schemas held in memory.
    //     A single static map should be good enough for most use cases however.
    //  2) The use of cached schemas means that changes made to schemas after
    //      they are first retrieved from the schema registry will not be
    //      reflected unless the compute node is restarted.
    //     If schema changes are anticipated, then you may want to replace these
    //      with a time-based map implementation that supports expiring keys.
    //

    /**
     * Store of message readers that are cached to allow re-use.
     *  The map is keyed by the schema id. See the comment above for
     *  design considerations about the use of this map.
     */
    private static final Map<Long, GenericDatumReader<GenericRecord>> CACHED_MESSAGE_READERS = new HashMap<>();
    /**
     * Store of Avro schemas that are cached to allow re-use.
     *  The map is keyed by the schema id. See the comment above for
     *  design considerations about the use of this map.
     */
    private static final Map<Long, Schema> CACHED_SCHEMAS = new HashMap<>();

    /** Instance of an Avro parser that is cached to allow re-use. */
    private static Parser parser;




    // ------------------------------------------------------------------------
    //  MESSAGE FLOW LIFECYCLE METHODS
    // ------------------------------------------------------------------------

    /**
     * onSetup() is called during the start of the message flow allowing
     * configuration to be read/cached, and endpoints to be registered.
     *
     * This will retrieve configuration from the {kafka}:schemaregistry policy
     * which will link this compute node to that policy.
     *
     * The impact is that any subsequent re-deploys to that policy will cause
     * message flows using this node to be torn down and re-initialized.
     */
    @Override
    public void onSetup() throws MbException {
        //
        // retrieve credentials for schema registry API calls from the policy
        //   and use it to generate an HTTP Authorization header
        //
        try {
            String auth = getSchemaRegistryUsername() + ":" + getSchemaRegistryPassword();
            byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
            SCHEMA_REGISTRY_AUTH_HEADER = "Basic " + new String(encodedAuth);
        }
        catch (DeserializationFailedException exc) {
            throw new MbUserException(this, "onSetup()", "", "", exc.toString(), null);
        }

        //
        // create an Avro schema parser
        //
        parser = new Parser();
    }


    /**
     * evaluate() is called when the compute node is invoked with a new message.
     */
    public void evaluate(MbMessageAssembly inAssembly) throws MbException {
        MbMessage inMessage = inAssembly.getMessage();
        try {
            // get raw bytes that were retrieved from the
            //  Kafka topic by a KafkaConsumer node
            byte[] msgbytes = inMessage.getBuffer();

            // get the schema ID from the message bytes
            Long schemaId = getSchemaId(msgbytes);

            // create a decoder based on the message bytes
            byte[] messagebytes = Arrays.copyOfRange(msgbytes, BYTES_IDX_MSGDATA, msgbytes.length);
            ByteArrayInputStream bais = new ByteArrayInputStream(messagebytes);
            Decoder decoder = getDecoder(schemaId, bais);

            // deserialize the message data using the decoder
            GenericDatumReader<GenericRecord> reader = getMessageReader(schemaId);
            GenericRecord record = reader.read(null, decoder);

            // create a new output JSON message with the deserialized data
            MbMessage outMessage = new MbMessage();
            copyMessageHeaders(inMessage, outMessage);
            outMessage
                .getRootElement()
                .createElementAsLastChildFromBitstream(record.toString().getBytes(),
                                                       MbJSON.PARSER_NAME, "", "", "", 0, 0, 0);
            MbMessageAssembly outAssembly = new MbMessageAssembly(inAssembly, outMessage);

            // send the deserialized json object to the output terminal
            MbOutputTerminal out = getOutputTerminal("out");
            out.propagate(outAssembly);

        } catch (DeserializationFailedException e) {
            // unable to deserialize the message data
            e.printStackTrace();

            // make a copy of the original message
            MbMessage outMessage = new MbMessage(inMessage);
            MbMessageAssembly altAssembly = new MbMessageAssembly(inAssembly, outMessage);

            // send the raw un-deserialized message to the alt terminal
            MbOutputTerminal alt = getOutputTerminal("alternate");
            alt.propagate(altAssembly);

        } catch (MbException e) {
            // Re-throw to allow Broker handling of MbException
            throw e;
        } catch (RuntimeException e) {
            // Re-throw to allow Broker handling of RuntimeException
            throw e;
        } catch (Exception e) {
            // Consider replacing Exception with type(s) thrown by user code
            // Example handling ensures all exceptions are re-thrown to be handled in the flow
            throw new MbUserException(this, "evaluate()", "", "", e.toString(), null);
        }
    }




    // ------------------------------------------------------------------------
    //  APP CONNECT ENTERPRISE LOGIC
    // ------------------------------------------------------------------------

    /** Copy headers from inMessage to outMessage. */
    public void copyMessageHeaders(MbMessage inMessage, MbMessage outMessage) throws MbException {
        MbElement outRoot = outMessage.getRootElement();
        MbElement header = inMessage.getRootElement().getFirstChild();

        while(header != null && header.getNextSibling() != null) {
            outRoot.addAsLastChild(header.copy());
            header = header.getNextSibling();
        }
    }




    // ------------------------------------------------------------------------
    //  BYTE PROCESSING LOGIC
    // ------------------------------------------------------------------------

    /**
     * Gets the schema ID from the provided Kafka message data.
     *
     *  Apicurio Registry clients store schema IDs at the start of the message
     *  payload data. This method reads the appropriate number of bytes from
     *  the start of the message and turns it into a long that can be used
     *  in schema registry URLs.
     */
    private Long getSchemaId(byte[] messageBytes) throws DeserializationFailedException {
        if (messageBytes.length == 0) {
            throw new DeserializationFailedException("Received empty message");
        }
        if (messageBytes[0] != 0) {
            throw new DeserializationFailedException("Received message without 'magic' byte");
        }
        if (messageBytes.length < BYTES_IDX_MSGDATA) {
            throw new DeserializationFailedException("Received message that is too small to process");
        }

        // read the correct sequence of bytes from the message data
        byte[] idBytes = Arrays.copyOfRange(messageBytes,
                BYTES_IDX_SCHEMAID,
                BYTES_IDX_SCHEMAID + SCHEMA_ID_BYTES_LEN);

        // convert into an id
        return convertBytesToLong(idBytes);
    }


    /** Utility function for converting bytes to a Long value. */
    private static long convertBytesToLong(final byte[] b) {
        long val = 0;
        for (int i = 0; i < SCHEMA_ID_BYTES_LEN; i++) {
            val <<= Byte.SIZE;
            val |= (b[i] & 0xFF);
        }
        return val;
    }




    // ------------------------------------------------------------------------
    //  SCHEMA REGISTRY API LOGIC
    // ------------------------------------------------------------------------

    // This is based on the Apicurio Schema Registry.
    //  If a different schema registry is used, then these methods will need
    //  to be adjusted to match the API for that registry.


    /**
     * Retrieve an Avro schema from the schema registry.
     *
     * If this is the first time the schema with this ID has been retrieved, it
     *  will be downloaded from the schema registry.
     *
     * Subsequent calls will return the schema from an in-memory map.
     */
    private Schema getSchema(Long schemaId) throws DeserializationFailedException, MbException  {
        if (CACHED_SCHEMAS.containsKey(schemaId) == false) {
            // this URL pattern is based on the Apicurio API docs
            String schemaUrl = getSchemaRegistryUrl() + "/ids/" + schemaId;

            // download the contents of the schema as a string
            String schemaSpec = downloadSchemaContents(schemaUrl);

            // parse the string into an Avro schema object
            Schema schema = parser.parse(schemaSpec);

            // cache the schema to allow reuse for subsequent messages
            CACHED_SCHEMAS.put(schemaId, schema);
        }

        return CACHED_SCHEMAS.get(schemaId);
    }


    /**
     * Download a schema from the provided URL. An Authorization header will
     *  be included in the request, using the credentials retrieved from the
     *  schemaregistry policy.
     */
    private String downloadSchemaContents(String url) throws DeserializationFailedException {
        try {
            URLConnection uc = new URL(url).openConnection();
            uc.setRequestProperty ("Authorization", SCHEMA_REGISTRY_AUTH_HEADER);
            try (InputStream is = uc.getInputStream()){
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String read = null;
                StringBuffer sb = new StringBuffer();
                while((read = br.readLine()) != null) {
                    sb.append(read);
                }
                return sb.toString();
            }
        }
        catch (IOException e) {
            throw new DeserializationFailedException("Failed to download schema from registry", e);
        }
    }




    // ------------------------------------------------------------------------
    //  APACHE AVRO LOGIC
    // ------------------------------------------------------------------------

    /**
     * Creates a message reader for consuming bytes from messages serialized
     *  using the schema with the specified id.
     */
    private GenericDatumReader<GenericRecord> getMessageReader(Long schemaId) throws DeserializationFailedException, MbException  {
        if (CACHED_MESSAGE_READERS.containsKey(schemaId) == false) {
            // retrieves the Avro schema to base the reader on
            Schema schema = getSchema(schemaId);

            // create a new reader using the schema
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

            // cache the reader to allow reuse for subsequent messages
            CACHED_MESSAGE_READERS.put(schemaId, reader);
        }

        return CACHED_MESSAGE_READERS.get(schemaId);
    }


    /**
     * Creates a decoder for decoding the provided message byte stream, for
     *  messages serialized using the schema with the provided id.
     */
    private Decoder getDecoder(Long schemaId, ByteArrayInputStream bais) throws DeserializationFailedException, MbException {
        if (ENCODING_APPROACH == null) {
            // retrieve the type of decoder to create
            //  from the schemaregistry policy
            ENCODING_APPROACH = getSchemaEncoding();
        }

        try {
            // create a binary decoder
            if (ENCODING_APPROACH == AvroEncodingApproach.BINARY) {
                return DecoderFactory.get().binaryDecoder(bais, null);
            }
            // create a JSON decoder
            else if (ENCODING_APPROACH == AvroEncodingApproach.JSON) {
                return DecoderFactory.get().jsonDecoder(getSchema(schemaId), bais);
            }
            // no other types are currently supported
            else {
                throw new DeserializationFailedException("Unable to identify decoder type");
            }
        } catch (IOException e) {
            throw new DeserializationFailedException("Unable to create decoder", e);
        }
    }




    // ------------------------------------------------------------------------
    //  READING CONFIG PARAMETERS FROM THE POLICY
    // ------------------------------------------------------------------------

    private String getSchemaRegistryUrl() throws DeserializationFailedException, MbException  {
        return getRequiredConfigProperty(POLICY_SCHEMA_REGISTRY_URL);
    }
    private String getSchemaRegistryUsername() throws DeserializationFailedException, MbException  {
        return getRequiredConfigProperty(POLICY_SCHEMA_REGISTRY_USERNAME);
    }
    private String getSchemaRegistryPassword() throws DeserializationFailedException, MbException  {
        return getRequiredConfigProperty(POLICY_SCHEMA_REGISTRY_PASSWORD);
    }
    private AvroEncodingApproach getSchemaEncoding() throws DeserializationFailedException, MbException {
        String encoding = getRequiredConfigProperty(POLICY_SCHEMA_ENCODING);
        switch (encoding) {
        case "json":
            return AvroEncodingApproach.JSON;
        case "binary":
            return AvroEncodingApproach.BINARY;
        default:
            throw new DeserializationFailedException("Unsupported encoding approach in policy parameter " +
                                                     POLICY_SCHEMA_ENCODING + ". " +
                                                     "Should be 'binary' or 'json'");
        }
    }

    /** Utility function for retrieving a string from the schema registry policy. */
    private String getRequiredConfigProperty(String parm) throws DeserializationFailedException, MbException {
        MbPolicy policy = getPolicy("UserDefined", "{kafka}:schemaregistry");
        if (policy == null) {
            throw new DeserializationFailedException("Missing required policy: {kafka}:schemaregistry");
        }
        String val = policy.getStringPropertyValue(parm);
        if (val == null || val.trim().length() == 0) {
            throw new DeserializationFailedException("{kafka}:schemaregistry policy is missing required parameter " + parm);
        }
        return val;
    }




    // ------------------------------------------------------------------------
    //  CUSTOM TYPES
    // ------------------------------------------------------------------------

    static class DeserializationFailedException extends Exception {
        private static final long serialVersionUID = 1L;
        public DeserializationFailedException(String msg) {
            super(msg);
        }
        public DeserializationFailedException(String msg, Throwable thr) {
            super(msg, thr);
        }
    }

    enum AvroEncodingApproach {
        /** Messages were encoded using a binary encoder, so a binary decoder is required to deserialize them. */
        BINARY,
        /** Messages were encoded using a JSON encoder, so a JSON decoder is required to deserialize them. */
        JSON;
    }
}
