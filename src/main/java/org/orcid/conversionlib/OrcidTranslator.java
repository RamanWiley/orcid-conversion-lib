package org.orcid.conversionlib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.Optional;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.orcid.conversionlib.CommandLineOptions.InputFormat;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.google.common.io.Resources;

/**
 * Utility class for serialising/deserialising ORCID records using the official
 * ORCID JAXB model.
 */
public class OrcidTranslator<T> {

    private final ObjectMapper mapper;
    private final Class<?> modelClass;
    private final Class<?> errorClass;

    private final JAXBContext jaxbContext;
    private final Schema schema; // may be null if validation disabled

    private final Unmarshaller unmarshaller;
    private final Marshaller marshaller;

    public static OrcidTranslator<org.orcid.jaxb.model.record_v2.Record> v2_0(boolean schemaValidate) {
        return new OrcidTranslator<>(SchemaVersion.V2_0, schemaValidate);
    }

    public static OrcidTranslator<org.orcid.jaxb.model.record_v2.Record> v2_1(boolean schemaValidate) {
        return new OrcidTranslator<>(SchemaVersion.V2_1, schemaValidate);
    }

    public static OrcidTranslator<org.orcid.jaxb.model.v3.rc1.record.Record> v3_0RC1(boolean schemaValidate) {
        return new OrcidTranslator<>(SchemaVersion.V3_0RC1, schemaValidate);
    }

    public static OrcidTranslator<org.orcid.jaxb.model.v3.release.record.Record> v3_0(boolean schemaValidate) {
        return new OrcidTranslator<>(SchemaVersion.V3_0, schemaValidate);
    }

    private OrcidTranslator(SchemaVersion location, boolean schemaValidate) {
        mapper = new ObjectMapper();
        mapper.registerModule(new JaxbAnnotationModule());
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        this.modelClass = location.modelClass;
        this.errorClass = location.errorClass;

        try {
            JAXBContext ctx = JAXBContext.newInstance(modelClass, errorClass);
            this.jaxbContext = ctx;

            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            URL url = Resources.getResource(location.location);

            Unmarshaller u = ctx.createUnmarshaller();
            Marshaller m = ctx.createMarshaller();

            Schema s = null;
            if (schemaValidate) {
                s = sf.newSchema(url);
                u.setSchema(s);
                m.setSchema(s);
            }

            this.schema = s;
            this.unmarshaller = u;
            this.marshaller = m;
        } catch (JAXBException | SAXException e) {
            throw new RuntimeException("Unable to create JAXB marshaller/unmarshaller", e);
        }
    }

    /**
     * Create a new Unmarshaller instance configured with the same schema as the
     * main translator. This is used in parallel tarball mode where each thread
     * needs its own Unmarshaller instance.
     */
    public Unmarshaller newUnmarshaller() throws JAXBException {
        Unmarshaller u = jaxbContext.createUnmarshaller();
        if (schema != null) {
            u.setSchema(schema);
        }
        return u;
    }

    public void translate(Optional<String> inputFilename,
                          Optional<String> outputFilename,
                          InputFormat inputFormat)
            throws FileNotFoundException, IOException,
                   JsonGenerationException, JsonMappingException,
                   JAXBException, JsonParseException {

        // Input
        Reader r;
        if (inputFilename.isPresent() && !inputFilename.get().isEmpty()) {
            File file = new File(inputFilename.get());
            r = new FileReader(file);
        } else {
            r = new InputStreamReader(System.in);
        }

        // Output
        Writer w;
        if (outputFilename.isPresent() && !outputFilename.get().isEmpty()) {
            File output = new File(outputFilename.get());
            w = new FileWriter(output);
        } else {
            w = new PrintWriter(System.out);
        }

        if (inputFormat.equals(InputFormat.XML)) {
            writeJsonRecord(w, readXmlRecord(r));
        } else {
            writeXmlRecord(w, readJsonRecord(r));
        }
    }

    @SuppressWarnings("unchecked")
    public T readJsonRecord(Reader reader)
            throws JsonParseException, JsonMappingException, IOException {
        return (T) mapper.readValue(reader, modelClass);
    }

    @SuppressWarnings("unchecked")
    public T readXmlRecord(Reader reader) throws JAXBException {
        return (T) unmarshaller.unmarshal(reader);
    }

    public void writeJsonRecord(Writer w, T r)
            throws JsonGenerationException, JsonMappingException, IOException {
        mapper.writeValue(w, r);
    }

    public void writeXmlRecord(Writer w, T r) throws JAXBException {
        marshaller.marshal(r, w);
    }
}
