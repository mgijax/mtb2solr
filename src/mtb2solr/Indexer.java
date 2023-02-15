/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mtb2solr;

/**
 *
 * @author not sbn
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient.Builder;

import org.apache.solr.common.SolrInputDocument;

public class Indexer {

    private boolean THREAD_LOG = true;
    private int failedThreads = 0;

    // Variables for handling threads
    private List<Thread> currentThreads = new ArrayList<Thread>();
    // maxThreads is configurable. When maxThreads is reached, program waits until they are finished.
    // This is essentially running them in batches
    private int maxThreads = 10;

    private static Http2SolrClient server = null;
    private static Indexer instance = null;

    public static Indexer getInstance(String httpUrl) {

        if (instance == null) {
            instance = new Indexer(httpUrl);
        }
        return instance;
    }

    private Indexer(String httpUrl) {

        Builder builder = new Builder(httpUrl);
        builder.connectionTimeout(100000);
       
        server = builder.build();
        //  server.setSoTimeout(100000); // socket read timeout
        //  server.setConnectionTimeout(200000); // upped to avoid IOExceptions
        server.setFollowRedirects(false); // defaults to false
        //  server.setAllowCompression(true);

        // set to use javabin format for faster indexing
        server.setRequestWriter(new BinaryRequestWriter());

    }

    public void delete() {
        try {
            System.out.println("Deleting current index.");
            server.deleteByQuery("*:*");
            server.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    /*
     * writes documents to solr.
     * Best practice is to write small batches of documents to Solr
     * and to commit less frequently. (TIP: this method will commit documents automatically using commitWithin)
     * Here we also spawn a new process for each batch of documents.
     */
    public void writeDocs(Collection<SolrInputDocument> docs) {
        DocWriterThread docWriter = new DocWriterThread(this, docs);
        Thread newThread = new Thread(docWriter);
        // kick off the thread
        newThread.start();
        // add to list of threads to monitor
        currentThreads.add(newThread);
        if (currentThreads.size() >= maxThreads) {
            // max thread pool size reached. Wait until they all finish, and clear the pool
            if (THREAD_LOG) {
                System.out.println("Max threads (" + maxThreads + ") reached. Waiting for all threads to finish.");
            }
            for (Thread t : currentThreads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    // not quite sure what to do here. Let's hope we don't see this error.
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }
            currentThreads = new ArrayList<Thread>();
        }
    }

    /* Prevents logging information related to threading */
    public void stopThreadLogging() {
        THREAD_LOG = false;
    }


    /*
     * Add documents to Solr in a thread to improve load time
     * This was investigated to be a bottleneck in large data loads
     */
    class DocWriterThread implements Runnable {

        Http2SolrClient server;
        Collection<SolrInputDocument> docs;
        private final int commitWithin = 50000; // 50 seconds
        private final int times_to_retry = 5;
        private int times_retried = 0;
        private final Indexer idx;

        public DocWriterThread(Indexer idx, Collection<SolrInputDocument> docs) {
            this.server = idx.server;
            this.docs = docs;
            this.idx = idx;
        }

        public void run() {
            try {
                // set the commitWithin feature
                server.add(docs, commitWithin);
            } catch (SolrServerException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                retry();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        }

        public void retry() {
            if (times_retried < times_to_retry) {
                times_retried++;
                System.out.println("retrying submit of stack of documents that failed");
                boolean succeeded = true;
                try {
                    // set the commitWithin feature
                    server.add(docs, commitWithin);
                } catch (SolrServerException e) {
                    succeeded = false;
                    System.out.println("failed");
                    e.printStackTrace();
                    retry();
                } catch (IOException e) {
                    succeeded = false;
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    retry();
                } catch (Exception e) {
                    succeeded = false;
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    // don't know what this exception is. Not retrying
                }
                if (succeeded) {
                    System.out.println("succeeded!");
                } else {
                    reportFailure();
                }
            } else {
                System.out.println("tried to re-submit stack of documents " + times_retried + " times. Giving up.");
                reportFailure();
            }
        }

        public void reportFailure() {
            this.idx.reportThreadFailure();
        }
    }

    // used by threads to alert when a thread failed.
    public void reportThreadFailure() {
        this.failedThreads += 1;
    }

    // returns true if any threads reported a failure
    public boolean hasFailedThreads() {
        return this.failedThreads > 0;
    }

}
