package ru.softlynx.gbq;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.*;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.log.LogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.tools.generic.*;
import org.apache.velocity.tools.view.WebappResourceLoader;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BqContext {
    public static String LOGGER_NAME = BqSelect.class.getSimpleName();
    static Logger logger = Logger.getLogger(LOGGER_NAME);
    static LogChute vlogger = new LogChute() {
        @Override
        public void init(RuntimeServices rs) throws Exception {

        }

        @Override
        public void log(int level, String message) {
            log(level, message, null);
        }

        @Override
        public void log(int level, String message, Throwable t) {
            Level llevel = Level.INFO;
            switch (level) {
                case DEBUG_ID:
                    llevel = Level.ALL;
                    break;

                case INFO_ID:
                    llevel = Level.INFO;
                    break;

                case WARN_ID:
                    llevel = Level.WARNING;
                    break;

                case ERROR_ID:
                    llevel = Level.SEVERE;
                    break;

            }
            logger.log(llevel, message, t);
        }

        @Override
        public boolean isLevelEnabled(int level) {
            return true;
        }
    };
    final VelocityContext vc;
    final VelocityEngine ve;
    final private String projectId;
    final private Bigquery bq;


    BqContext(Builder builder) throws GeneralSecurityException, IOException, URISyntaxException {
        this.projectId = builder.projectId;
        final GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(new NetHttpTransport())
                .setJsonFactory(new JacksonFactory())
                .setServiceAccountId(builder.serviceAccount)
                .setServiceAccountScopes(Collections.singletonList(BigqueryScopes.BIGQUERY))
                .setServiceAccountPrivateKeyFromP12File(new File(builder.keyURI.toURI()))
                .build();

        bq = new Bigquery.Builder(
                credential.getTransport(),
                credential.getJsonFactory(),
                credential)
                .setApplicationName("ru.softlynx.gbq")
                .setHttpRequestInitializer(credential)
                .build();

        ve = new VelocityEngine();
        if (builder.servletContext != null) {
            ve.setApplicationAttribute("javax.servlet.ServletContext", builder.servletContext);
        }
        //ve.setProperty(VelocityEngine.RUNTIME_LOG_LOGSYSTEM, vlogger);
        ve.setProperty(VelocityEngine.INPUT_ENCODING, "utf-8");
        ve.setProperty(VelocityEngine.OUTPUT_ENCODING, "utf-8");
        ve.setProperty(VelocityEngine.RESOURCE_LOADER, "classpath,webapp");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getCanonicalName());
        ve.setProperty("classpath.resource.loader.path", "/");
        ve.setProperty("webapp.resource.loader.class", WebappResourceLoader.class.getCanonicalName());
        ve.setProperty("webapp.resource.loader.path", "/WEB-INF/");
        ve.init();
        for (String lib : builder.libs) {
            ve.getTemplate(lib);
        }
        vc = new VelocityContext(builder.vc);
        vc.put("esc", new EscapeTool());
        vc.put("date", new DateTool());
        vc.put("math", new MathTool());
        vc.put("number", new NumberTool());
        vc.put("parser", new ValueParser());
        vc.put("sorter", new SortTool());
    }

    Bigquery BQ() {
        return bq;
    }

    String PROJECT_ID() {
        return projectId;
    }

    public BqSelect.Builder select(String queryName, Object... params) {
        return new BqSelect.Builder(this)
                .withTemplateMacro(queryName)
                .withParams(params);
    }

    public BqSelect fromJobID(String jobid) throws IOException {
        if ((jobid == null) || (jobid.isEmpty())) {
            return null;
        }
        return new BqSelect(new BqSelect.Builder(this, jobid));
    }

    public TableDataInsertAllResponse insert(String dataset, String tablename, TableDataInsertAllRequest content)
            throws IOException {
        TableDataInsertAllResponse response = bq.tabledata()
                .insertAll(PROJECT_ID(),
                        dataset,
                        tablename,
                        content).execute();
        if ((response.getInsertErrors() != null) && (!response.getInsertErrors().isEmpty())) {
            throw new IOException(response.toPrettyString());
        }
        return response;
    }

    public void createDataset(String datasetName, String description) throws IOException {
        Dataset dataset = new Dataset();
        DatasetReference datasetRef = new DatasetReference();
        datasetRef.setProjectId(PROJECT_ID());
        datasetRef.setDatasetId(datasetName);
        dataset.setDatasetReference(datasetRef);
        if (description != null) {
            dataset.setFriendlyName(description);
            dataset.setDescription(description);
        }
        try {
            bq.datasets().insert(
                    PROJECT_ID(),
                    dataset
            ).execute();
        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409) { //Dataset table exists
                throw ex;
            }
        }
    }

    public Table getTableInfo(String datasetName, String tableName) {
        try {
            return bq.tables().get(
                    PROJECT_ID(),
                    datasetName,
                    tableName)
                    .execute();
        } catch (Exception ex) {
            return null;
        }
    }

    public String createTable(String datasetName, String tableName, TableSchema schema) throws IOException {
        logger.log(Level.INFO, "Creating new namespace " + tableName);
        Table table = new Table();
        table.setSchema(schema);
        TableReference tableRef = new TableReference();
        tableRef.setDatasetId(datasetName);
        tableRef.setProjectId(PROJECT_ID());
        tableRef.setTableId(tableName);
        table.setTableReference(tableRef);
        Bigquery.Tables.Insert rq = bq.tables().insert(
                PROJECT_ID(),
                datasetName,
                table);
        try {
            table = rq.execute();
        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409) { //Data table exists
                throw ex;
            }
        }
        logger.log(Level.INFO, "Table created " + table.getSelfLink());
        return tableName;
    }

    public void with(String key, Object value) {
        vc.put(key, value);
    }

    public void without(String key) {
        vc.remove(key);
    }

    public static class Builder {
        final VelocityContext vc = new VelocityContext();
        String projectId;
        HashSet<String> libs = new HashSet<String>();
        private Object servletContext = null;
        private String serviceAccount;
        private URL keyURI;

        public Builder() {
        }

        public Builder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withServiceAccount(String serviceAccount) {
            this.serviceAccount = serviceAccount;
            return this;
        }

        public Builder withP12KeyURL(URL keyURI) {
            this.keyURI = keyURI;
            return this;
        }

        public Builder withTemplateLibrary(String... libs) {
            this.libs.addAll(Arrays.asList(libs));
            return this;
        }

        public Builder with(String key, Object value) {
            vc.put(key, value);
            return this;
        }

        public BqContext build() throws GeneralSecurityException, IOException, URISyntaxException {
            return new BqContext(this);
        }

        public Builder withServletContext(Object servletContext) {
            this.servletContext = servletContext;
            return this;
        }
    }


}
