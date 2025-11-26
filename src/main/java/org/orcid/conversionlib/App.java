package org.orcid.conversionlib;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import javax.xml.bind.JAXBException;

import org.orcid.conversionlib.CommandLineOptions.InputFormat;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.devtools.common.options.OptionsParser;

/**
 * Main CLI entrypoint for ORCID conversion.
 *
 * Supports single-file mode and parallel tarball mode.
 */
public class App {
    private static final OptionsParser optionsParser =
            OptionsParser.newOptionsParser(CommandLineOptions.class);

    public static void main(String[] args) {
        if (args == null) {
            printUse();
            return;
        }

        optionsParser.parseAndExitUponError(args);
        CommandLineOptions options = optionsParser.getOptions(CommandLineOptions.class);

        try {
            // https://java.net/jira/browse/JAXB-895
            System.setProperty("com.sun.xml.bind.v2.bytecode.ClassTailor.noOptimize", "true");

            OrcidTranslator<?> t = null;
            switch (options.schemaVersion) {
                case V2_0:
                    t = OrcidTranslator.v2_0(options.schemaValidate);
                    break;
                case V2_1:
                    t = OrcidTranslator.v2_1(options.schemaValidate);
                    break;
                case V3_0RC1:
                    t = OrcidTranslator.v3_0RC1(options.schemaValidate);
                    break;
                case V3_0:
                    t = OrcidTranslator.v3_0(options.schemaValidate);
                    break;
                default:
                    System.err.println("Unsupported schema version: " + options.schemaVersion);
                    return;
            }

            if (options.tarball) {
                if (options.inputFormat.equals(InputFormat.JSON)) {
                    System.err.println("tarball mode only supports XML input format");
                    return;
                }
                int threads = options.threads <= 0
                        ? Runtime.getRuntime().availableProcessors()
                        : options.threads;

                ParallelOrcidArchiveTranslator<?> at =
                        new ParallelOrcidArchiveTranslator<>(t, threads);
                at.translate(options.fileName, options.outputFileName);
            } else {
                t.translate(
                        Optional.ofNullable(options.fileName),
                        Optional.ofNullable(options.outputFileName),
                        options.inputFormat
                );
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + options.fileName);
        } catch (JsonGenerationException e) {
            System.err.println("Could not write JSON: " + e);
        } catch (JsonMappingException e) {
            System.err.println("Could not read JSON: " + e);
        } catch (IOException e) {
            System.err.println("Could not read or write file: " + e);
        } catch (JAXBException e) {
            System.err.println("Could not parse XML: " + e);
        }
    }

    public static void printUse() {
        System.out.println("Usage: java -jar orcid-conversion-lib-<version>-full.jar OPTIONS");
        System.out.println(optionsParser.describeOptions(
                Collections.<String, String>emptyMap(),
                OptionsParser.HelpVerbosity.LONG));
    }
}
