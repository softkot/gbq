package ru.softlynx.gbq;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class BqSelect implements Iterator<BqSelect.Row>, Iterable<BqSelect.Row> {
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

        private TableRow row;

        public Row(TableRow row) {
            this.row = row;
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
        public static final String PRIO_INTERACTIVE = "INTERACTIVE";
        public static final String PRIO_BATCH = "BATCH";

        private final String sql;
        private final BqContext context;
        private String priority = PRIO_INTERACTIVE;
        private Boolean cache = true;
        private Long pageSize = 1000L;
        private String jobid;

        Builder(BqContext bqContext, String sql) {
            this.context = bqContext;
            this.sql = sql;

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


        public BqSelect build() throws IOException {
            if (pageSize <= 0) {
                pageSize = 1000L;
            }
            Job job = new Job();
            JobConfiguration config = new JobConfiguration();
            JobConfigurationQuery queryConfig = new JobConfigurationQuery();
            queryConfig.setPriority(priority);
            queryConfig.setUseQueryCache(cache);
            queryConfig.setQuery(sql);
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
            return sql;
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
