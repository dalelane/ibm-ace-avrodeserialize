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
 *  {policies}:schemaregistry policy.
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
	//        <schema.registry.id.length>8</schema.registry.id.length>
    //      </policy>
    //    </policies>
    //

	// required parameters

    /** Key for retrieving the URL for a schema registry from the {policies}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_REGISTRY_URL      = "schema.registry.url";
    /** Key for retrieving the username for schema registry API calls from the {policies}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_REGISTRY_USERNAME = "schema.registry.username";
    /** Key for retrieving the password for schema registry API calls from the {policies}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_REGISTRY_PASSWORD = "schema.registry.password";

    // optional parameters

    /** Key for retrieving the binary/json encoding setting from the {policies}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_ENCODING          = "schema.registry.encoding";
    /** Key for retrieving the length of IDs from the {policies}:schemaregistry policy. **/
    private static final String POLICY_SCHEMA_ID_LENGTH_BYTES   = "schema.registry.id.length";


    // values retrieved from the policy

    /**
     * Base URL for retrieving schemas from an Apicurio schema registry.
     *  Schema IDs should be appended to this to create a complete URL.
     */
    private String schemaRegistryBaseUrl = "";

    /**
     * Authorization HTTP header to use in API requests to the schema registry.
     */
    private String schemaRegistryAuthHeader = "";

    /**
     * The default decoding approach to use when deserializing Kafka message
     *  data if there are no message headers specifying what to do.
     */
    private AvroEncodingApproach defaultEncodingApproach = null;

    /**
     * The number of bytes used in Kafka messages to store the schema ID.
     *
     * The default assumes that Kafka messages were produced with a schema ID
     * handler set to io.apicurio.registry.serde.DefaultIdHandler which uses
     * an eight-byte long to encode schema IDs
     *
     * cf. https://www.apicur.io/registry/docs/apicurio-registry/2.1.0.Final/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-types-serde-registry
     *
     * If producing applications use the schema ID handler
     *  io.apicurio.registry.serde.Legacy4ByteIdHandler this uses a
     *  four-byte integer to encode schema IDs, they would use 4 to
     *  represent Integer.BYTES instead.
     */
    private int schemaIdBytesLength = Long.BYTES;


    // ------------------------------------------------------------------------
    //  BYTE-ARRAY OFFSETS - for processing message payloads
    // ------------------------------------------------------------------------

    // If the ID is stored within the message payload {@link SchemaIdLocation#PAYLOAD}
    //  the expected message data will be:
    //
    //  with an 8-byte (Long.BYTES) id:
	//   byte  0   : should contain 0
    //   bytes 1-9 : should contain the schema id as an 8-byte long
    //   bytes 10- : should contain the serialized message data
    //
    //  with a 4-byte (Integer.BYTES) id:
	//   byte  0   : should contain 0
    //   bytes 1-5 : should contain the schema id as a 4-byte int
    //   bytes 6-  : should contain the serialized message data

    /** Index into the message bytes for the location of the magic byte. */
    private static final int BYTES_IDX_MAGICBYTE = 0;
    /** Index into the message bytes for the start of the schema id. */
    private static final int BYTES_IDX_SCHEMAID  = BYTES_IDX_MAGICBYTE + 1;
    /** Index into the message bytes for the start of the serialized message contents. */
    private int              BYTES_IDX_MSGDATA   = BYTES_IDX_SCHEMAID + schemaIdBytesLength;




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
     * This will retrieve configuration from the {policies}:schemaregistry policy
     * which will link this compute node to that policy.
     *
     * The impact is that any subsequent re-deploys to that policy will cause
     * message flows using this node to be torn down and re-initialized.
     */
    @Override
    public void onSetup() throws MbException {
        try {
            // retrieve credentials for schema registry API calls from the policy
            //   and use it to generate an HTTP Authorization header
            String auth = getSchemaRegistryUsername() + ":" + getSchemaRegistryPassword();
            byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
            schemaRegistryAuthHeader = "Basic " + new String(encodedAuth);

            // base URL for fetching schemas - needs the schema ID appended
            //  to the end to be used
            schemaRegistryBaseUrl = getSchemaRegistryUrl() + "/ids/";

            // default approach to use for deserializing (binary vs json)
            //  but this can be overridden on a per-message basis through
            //  message headers
            defaultEncodingApproach = getDefaultSchemaEncoding();

            // get length of IDs
            schemaIdBytesLength = getSchemaIdLength();
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
        	AvroConfig config = getConfigFromHeaders(inAssembly.getLocalEnvironment().getRootElement());

            // get raw bytes that were retrieved from the
            //  Kafka topic by a KafkaConsumer node
            byte[] msgbytes = inMessage.getBuffer();

            // identify what subset of the bytes should be
            //  deserialized
            byte[] messagebytes;
            if (config.idLocation == SchemaIdLocation.PAYLOAD) {
                // if there was no schema ID provided in the header
                //   get it from the message bytes, and look at the
            	//   bytes following that as message payload
            	config.schemaId = getSchemaId(msgbytes);
            	messagebytes = Arrays.copyOfRange(msgbytes, BYTES_IDX_MSGDATA, msgbytes.length);
            }
            else { // idLocation == HEADER
            	messagebytes = msgbytes;
            }

            // create a decoder based on the message bytes
            ByteArrayInputStream bais = new ByteArrayInputStream(messagebytes);
            Decoder decoder = getDecoder(config, bais);

            // deserialize the message data using the decoder
            GenericDatumReader<GenericRecord> reader = getMessageReader(config.schemaId);
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

    /**
     * Kafka messages can include information about how to deserialize the data
     *  in message headers. This method looks that up using the ACE
     *  representation of kafka headers.
     */
    private AvroConfig getConfigFromHeaders(MbElement environmentRoot) throws MbException {
    	Long schemaId = null;
    	MbElement idHeader = environmentRoot.getFirstElementByPath("/Kafka/Input/KafkaHeader/apicurio.value.globalId");
    	if (idHeader != null) {
	    	byte[] idHeaderBytes = ((String)idHeader.getValue()).getBytes();
	    	schemaId = convertBytesToLong(idHeaderBytes);
    	}

    	AvroEncodingApproach encoding = null;
    	MbElement encodingHeader = environmentRoot.getFirstElementByPath("/Kafka/Input/KafkaHeader/apicurio.value.encoding");
    	if (encodingHeader != null) {
	    	String encodingHeaderValue = encodingHeader.getValueAsString();
	    	encoding = "JSON".equals(encodingHeaderValue) ? AvroEncodingApproach.JSON : AvroEncodingApproach.BINARY;
    	}

    	return new AvroConfig(schemaId, encoding);
    }


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
                BYTES_IDX_SCHEMAID + schemaIdBytesLength);

        // convert into an id
        return convertBytesToLong(idBytes);
    }


    /** Utility function for converting bytes to a Long value. */
    private long convertBytesToLong(final byte[] b) {
        long val = 0;
        for (int i = 0; i < schemaIdBytesLength; i++) {
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
            String schemaUrl = schemaRegistryBaseUrl + schemaId;

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
            uc.setRequestProperty ("Authorization", schemaRegistryAuthHeader);
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
    private Decoder getDecoder(AvroConfig config, ByteArrayInputStream bais) throws DeserializationFailedException, MbException {

    	AvroEncodingApproach encoding = config.encoding;
        if (encoding == null) {
        	// if not specified in the message header,
        	//  use the default from the policy
            encoding = defaultEncodingApproach;
        }

        try {
            // create a binary decoder
            if (encoding == AvroEncodingApproach.BINARY) {
                return DecoderFactory.get().binaryDecoder(bais, null);
            }
            // create a JSON decoder
            else if (encoding == AvroEncodingApproach.JSON) {
                return DecoderFactory.get().jsonDecoder(getSchema(config.schemaId), bais);
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
        return getRequiredStringConfig(POLICY_SCHEMA_REGISTRY_URL);
    }
    private String getSchemaRegistryUsername() throws DeserializationFailedException, MbException  {
        return getRequiredStringConfig(POLICY_SCHEMA_REGISTRY_USERNAME);
    }
    private String getSchemaRegistryPassword() throws DeserializationFailedException, MbException  {
        return getRequiredStringConfig(POLICY_SCHEMA_REGISTRY_PASSWORD);
    }
    private AvroEncodingApproach getDefaultSchemaEncoding() throws DeserializationFailedException, MbException {
        String encoding = getStringConfig(POLICY_SCHEMA_ENCODING, "binary");
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
    private int getSchemaIdLength() throws DeserializationFailedException, MbException {
    	return getIntConfig(POLICY_SCHEMA_ID_LENGTH_BYTES, Long.BYTES);
    }

    /** Utility function for retrieving a string from the schema registry policy. */
    private String getRequiredStringConfig(String parm) throws DeserializationFailedException, MbException {
        MbPolicy policy = getPolicy();
        if (policy.getPropertyNames().contains(parm)) {
        	String val = policy.getStringPropertyValue(parm);
        	if (val.trim().length() > 0) {
        		return val;
        	}
        }
        throw new DeserializationFailedException("{policies}:schemaregistry policy is missing required parameter " + parm);
    }
    /** Utility function for retrieving a string from the schema registry policy. */
    private String getStringConfig(String parm, String defaultValue) throws DeserializationFailedException, MbException {
        MbPolicy policy = getPolicy();
        if (policy.getPropertyNames().contains(parm)) {
        	String val = policy.getStringPropertyValue(parm);
        	if (val.trim().length() > 0) {
        		return val;
        	}
        }
        return defaultValue;
    }
    /** Utility function for retrieving an integer from the schema registry policy. */
    private Integer getIntConfig(String parm, Integer defaultValue) throws DeserializationFailedException, MbException {
        MbPolicy policy = getPolicy();
        if (policy.getPropertyNames().contains(parm)) {
        	Integer val = policy.getIntegerPropertyValue(parm);
	        return val;
        }
        return defaultValue;
    }
    private MbPolicy getPolicy() throws MbException, DeserializationFailedException {
    	MbPolicy policy = getPolicy("UserDefined", "{policies}:schemaregistry");
        if (policy == null) {
            throw new DeserializationFailedException("Missing required policy: {policies}:schemaregistry");
        }
        return policy;
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


    private class AvroConfig {
    	final SchemaIdLocation idLocation;
    	Long schemaId;
    	final AvroEncodingApproach encoding;

    	AvroConfig(Long idFromHeader, AvroEncodingApproach encoding) {
    		this.schemaId = idFromHeader;
    		idLocation = idFromHeader == null ? SchemaIdLocation.PAYLOAD : SchemaIdLocation.HEADER;
    		this.encoding = encoding == null ? defaultEncodingApproach : encoding;
    	}
    }


    enum AvroEncodingApproach {
        /** Messages were encoded using a binary encoder, so a binary decoder is required to deserialize them. */
        BINARY,
        /** Messages were encoded using a JSON encoder, so a JSON decoder is required to deserialize them. */
        JSON;
    }

    enum SchemaIdLocation {
    	HEADER,
    	PAYLOAD;
    }
}