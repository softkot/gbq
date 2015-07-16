package ru.softlynx.gbq;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.log.LogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.apache.velocity.tools.generic.*;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BqContext {
    private final VelocityContext vc;
    private final VelocityEngine ve;

    final private String projectId;
    final private Bigquery bq;

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

    public static class Builder {
        String projectId;
        private String serviceAccount;
        private URL keyURI;
        HashSet<String> libs = new HashSet<String>();

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

        public BqContext build() throws GeneralSecurityException, IOException, URISyntaxException {
            return new BqContext(this);
        }
    }

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
        //ve.setProperty(VelocityEngine.RUNTIME_LOG_LOGSYSTEM, vlogger);
        ve.setProperty(VelocityEngine.INPUT_ENCODING, "utf-8");
        ve.setProperty(VelocityEngine.OUTPUT_ENCODING, "utf-8");
        ve.setProperty(VelocityEngine.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getCanonicalName());
        ve.init();
        for (String lib : builder.libs) {
            ve.getTemplate(lib);
        }
        vc = new VelocityContext();
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
        StringWriter sw = new StringWriter();
        vc.put(queryName, params);
        String[] paramNames = null;
        if (params != null) {
            paramNames = new String[params.length];
            for (int i = 0; i < params.length; i++) {
                paramNames[i] = "${" + queryName + "[" + i + "]}";
            }
        }
        ve.invokeVelocimacro(queryName, queryName, paramNames, vc, sw);
        vc.remove(queryName);
        return new BqSelect.Builder(this, sw.toString());
    }

    public void put(String key, Object value) {
        vc.put(key, value);
    }

    public void remove(String key) {
        vc.remove(key);
    }


}
