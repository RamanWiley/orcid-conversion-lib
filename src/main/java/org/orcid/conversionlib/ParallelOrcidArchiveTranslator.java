package org.orcid.conversionlib;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

/**
 * Parallel tarball converter:
 *  - input: tar.gz with XML records
 *  - output: tar.gz with JSON records
 *
 * Uses a producer/worker/writer pipeline:
 *  - Producer reads entries and submits work to a thread pool
 *  - Workers convert XML -> Java -> JSON
 *  - Writer serializes results back into a tar.gz
 */
public class ParallelOrcidArchiveTranslator<T> {

    private final OrcidTranslator<T> translator;
    private final int numWorkers;

    public ParallelOrcidArchiveTranslator(OrcidTranslator<T> translator, int numWorkers) {
        this.translator = translator;
        this.numWorkers = numWorkers > 0
                ? numWorkers
                : Runtime.getRuntime().availableProcessors();
    }

    private static class JobResult {
        final long id;
        final String name;
        final byte[] data;   // null for directory or error
        final boolean directory;
        final Exception error;

        private JobResult(long id, String name, byte[] data, boolean directory, Exception error) {
            this.id = id;
            this.name = name;
            this.data = data;
            this.directory = directory;
            this.error = error;
        }

        static JobResult directory(long id, String name) {
            return new JobResult(id, name, null, true, null);
        }

        static JobResult success(long id, String name, byte[] data) {
            return new JobResult(id, name, data, false, null);
        }

        static JobResult failure(long id, String name, Exception e) {
            return new JobResult(id, name, null, false, e);
        }
    }

    public void translate(String in, String out) {
        System.out.println(new Date() + " starting parallel tarball conversion with " + numWorkers + " workers");

        ExecutorService pool = Executors.newFixedThreadPool(numWorkers);
        BlockingQueue<JobResult> queue = new LinkedBlockingQueue<>(numWorkers * 4);
        AtomicLong submitted = new AtomicLong(0);

        try (InputStream fi = Files.newInputStream(Paths.get(in));
             InputStream bi = new BufferedInputStream(fi);
             InputStream gzi = new GzipCompressorInputStream(bi);
             TarArchiveInputStream tin = new TarArchiveInputStream(gzi);

             OutputStream fo = Files.newOutputStream(Paths.get(out));
             OutputStream gzo = new GzipCompressorOutputStream(fo);
             TarArchiveOutputStream tout = new TarArchiveOutputStream(gzo)) {

            ArchiveEntry entry;
            long count = 0;
            long errorCount = 0;

            // Producer: read entries and submit work
            while ((entry = tin.getNextEntry()) != null) {
                if (!tin.canReadEntryData(entry)) {
                    System.err.println("Problem reading " + entry.getName());
                    continue;
                }

                final long jobId = submitted.getAndIncrement();

                if (entry.isDirectory()) {
                    queue.put(JobResult.directory(jobId, entry.getName()));
                } else {
                    final String entryName = entry.getName();
                    final byte[] xmlBytes = IOUtils.toByteArray(tin);

                    pool.submit(() -> {
                        try {
                            Unmarshaller u = translator.newUnmarshaller();
                            @SuppressWarnings("unchecked")
                            T rec = (T) u.unmarshal(new ByteArrayInputStream(xmlBytes));

                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            OutputStreamWriter w =
                                    new OutputStreamWriter(baos, StandardCharsets.UTF_8);
                            translator.writeJsonRecord(w, rec);
                            w.flush();

                            String outName = entryName.replaceAll("\.xml$", ".json");
                            queue.put(JobResult.success(jobId, outName, baos.toByteArray()));
                        } catch (Exception e) {
                            try {
                                queue.put(JobResult.failure(jobId, entryName, e));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    });
                }

                count++;
                if (count % 10000 == 0) {
                    System.out.println(new Date() + " submitted " + count + " entries");
                }
            }

            pool.shutdown();

            long expected = submitted.get();
            long written = 0;

            // Writer: drain queue and write results into tar.gz
            while (written < expected || !pool.isTerminated()) {
                JobResult r = queue.poll(1, TimeUnit.SECONDS);
                if (r == null) {
                    continue;
                }
                written++;

                if (r.error != null) {
                    errorCount++;
                    System.err.println("Problem processing file " + errorCount + " " + r.name + " " + r.error.getMessage());
                    continue;
                }

                if (r.directory) {
                    TarArchiveEntry dirEntry = new TarArchiveEntry(r.name);
                    dirEntry.setSize(0);
                    tout.putArchiveEntry(dirEntry);
                    tout.closeArchiveEntry();
                } else {
                    TarArchiveEntry outEntry = new TarArchiveEntry(r.name);
                    outEntry.setSize(r.data.length);
                    tout.putArchiveEntry(outEntry);
                    tout.write(r.data);
                    tout.closeArchiveEntry();
                }

                if (written % 10000 == 0) {
                    System.out.println(new Date() + " written " + written + " entries");
                    tout.flush();
                }
            }

            System.out.println(new Date() + " finished. entries=" + submitted.get() + " errors=" + errorCount);
            tout.finish();
        } catch (IOException | InterruptedException e) {
            System.err.println("Problem processing files");
            e.printStackTrace();
        } finally {
            pool.shutdownNow();
        }
    }
}
