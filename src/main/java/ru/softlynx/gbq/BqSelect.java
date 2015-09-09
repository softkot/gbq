package ru.softlynx.gbq;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import org.apache.velocity.VelocityContext;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class BqSelect implements Iterator<BqSelect.Row>, Iterable<BqSelect.Row> {
    public static final String PRIO_INTERACTIVE = "INTERACTIVE";
    public static final String PRIO_BATCH = "BATCH";

    private final String jobid;
    private final Bigquery.Jobs.GetQueryResults query;
    private String pagetoken = null;
    private final BqContext context;
    private GetQueryResultsResponse response;
    Iterator<TableRow> rows = null;
    HashMap<String, Integer> columns = null;

    public GetQueryResultsResponse getResponse() throws IOException {
        if (response == null) {
            query.setPageToken(pagetoken);
            response = query.execute();
        }
        return response;
    }

    public HashMap<String, Integer> getColumnIndexes() throws IOException {
        if (columns == null) {
            columns = new HashMap<String, Integer>();
            int columnIndex = 0;
            for (TableFieldSchema s : getResponse().getSchema().getFields()) {
                columns.put(s.getName(), columnIndex++);
            }
        }
        return columns;
    }

    @Override
    public Iterator<Row> iterator() {
        return this;
    }

    public class Row {

        private final List<TableCell> cells;


        public List<TableCell> getCells() {
            return cells;
        }

        public Row(TableRow row) {
            this.cells = row.getF();
        }

        public Object asObject(Integer idx) {
            Object obj = cells.get(idx).getV();
            return Data.isNull(obj) ? null : obj;
        }

        public Object asObject(String name) throws IOException {
            return asObject(getColumnIndexes().get(name));
        }

        public String asString(Integer idx) {
            Object obj = asObject(idx);
            return obj == null ? null : obj.toString();
        }

        public String asString(String name) throws IOException {
            Object obj = asObject(name);
            return obj == null ? null : obj.toString();
        }

        public Long asLong(Integer idx) {
            Object obj = asObject(idx);
            return obj == null ? null : Long.parseLong(obj.toString());
        }

        public Long asLong(String name) throws IOException {
            Object obj = asObject(name);
            return obj == null ? null : Long.parseLong(obj.toString());
        }

        public Integer asInteger(Integer idx) {
            Object obj = asObject(idx);
            return obj == null ? null : Integer.parseInt(obj.toString());
        }

        public Integer asInteger(String name) throws IOException {
            Object obj = asObject(name);
            return obj == null ? null : Integer.parseInt(obj.toString());
        }

        public Double asDouble(Integer idx) {
            Object obj = asObject(idx);
            return obj == null ? null : Double.parseDouble(obj.toString());
        }

        public Double asDouble(String name) throws IOException {
            Object obj = asObject(name);
            return obj == null ? null : Double.parseDouble(obj.toString());
        }

        public Date asDate(Integer idx) {
            Double d = asDouble(idx);
            return d == null ? null : new Date((long) d.doubleValue());
        }

        public Date asDate(String name) throws IOException {
            Double d = asDouble(name);
            return d == null ? null : new Date((long) d.doubleValue());
        }

        public boolean isNull(Integer idx) {
            return asObject(idx) == null;
        }

        public boolean isNull(String name) throws IOException {
            return asObject(name) == null;
        }

    }

    @Override
    public boolean hasNext() {

        if (rows != null) {
            if (rows.hasNext()) {
                return true;
            }
            if (response != null) {
                pagetoken = response.getPageToken();
                rows = null;
                if (pagetoken == null) {
                    return false;
                }
            }
        }

        if (rows == null) {
            try {
                response = null;
                if (getResponse().getRows() == null) {
                    return false;
                }
                rows = getResponse().getRows().iterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return rows.hasNext();
    }

    @Override
    public Row next() {
        return (rows == null) ? null : new Row(rows.next());
    }

    public static class Builder {

        private final BqContext context;
        private final VelocityContext localvc;
        private String priority = PRIO_INTERACTIVE;
        private Boolean cache = true;
        private Long pageSize = 1000L;
        private String jobid;
        private String macroName;
        private Object[] params;
        private String sql;

        Builder(BqContext bqContext) {
            this.context = bqContext;
            localvc = new VelocityContext(bqContext.vc);
        }

        public Builder useCache(Boolean cache) {
            this.cache = cache;
            return this;
        }

        public Builder withPriority(String priority) {
            this.priority = priority;
            return this;
        }

        public Builder withPageSize(Long pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder withPageSize(Integer pageSize) {
            this.pageSize = Long.valueOf(pageSize);
            return this;
        }

        public Builder with(String key, Object value) {
            localvc.put(key, value);
            return this;
        }


        public BqSelect build() throws IOException {
            if (pageSize <= 0) {
                pageSize = 1000L;
            }
            Job job = new Job();
            JobConfiguration config = new JobConfiguration();
            JobConfigurationQuery queryConfig = new JobConfigurationQuery();
            queryConfig.setPriority(priority);
            queryConfig.setUseQueryCache(cache);
            queryConfig.setQuery(getSql());
            config.setQuery(queryConfig);
            job.setConfiguration(config);
            Bigquery.Jobs.Insert insert = context.BQ().jobs().insert(
                    context.PROJECT_ID(),
                    job);
            insert.setProjectId(context.PROJECT_ID());
            jobid = insert.execute().getJobReference().getJobId();
            return new BqSelect(this);
        }

        public String getSql() {
            if (sql == null) {
                StringWriter sw = new StringWriter();
                localvc.put(macroName, params);
                String[] paramNames = null;
                if (params != null) {
                    paramNames = new String[params.length];
                    for (int i = 0; i < params.length; i++) {
                        paramNames[i] = "${" + macroName + "[" + i + "]}";
                    }
                }
                context.ve.invokeVelocimacro(macroName, macroName, paramNames, localvc, sw);
                sql = sw.toString();
            }
            return sql;
        }

        Builder withTemplateMacro(String macroName) {
            this.macroName = macroName;
            return this;
        }

        Builder withParams(Object... params) {
            this.params = params;
            return this;
        }

    }

    BqSelect(Builder builder) throws IOException {
        context = builder.context;
        this.jobid = builder.jobid;
        query = context.BQ().jobs().getQueryResults(context.PROJECT_ID(), jobid);
        query.setMaxResults(builder.pageSize);

    }

    public BqContext getContext() {
        return context;
    }

    public Long getPageSize() {
        return query.getMaxResults();
    }

    public String getJobid() {
        return jobid;
    }

    public JobStatus getStatus() throws IOException {
        Job pollJob = context.BQ().jobs().get(context.PROJECT_ID(), jobid).execute();
        return pollJob.getStatus();
    }

    public boolean isCompleted() throws IOException {
        return getStatus().getState().equals("DONE");
    }

}
