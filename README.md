# Google BigQuery wrapper 

## Simplify access to BigQuery with power of Apache velocity templated SQL and paginated results.

Use it in a next simple steps.

### Create SQL template resource file (sql.vm)

    #macro(alltabledata $table)
    SELECT * FROM [${DATASET}.$table]
    #end

### Create context object

    BqContext context = new BqContext.Builder()
                .withProjectId("my-big-query-project")
                .withServiceAccount("272433143339-rs14caqptempvr50reqn5mc6b41mb655@developer.gserviceaccount.com")
                .withP12KeyURL(this.getClass().getResource("service-account-key.p12"))
                .withTemplateLibrary("sql.vm")
                .build();

### Populate context with some tenplate variables

    context.put("DATASET", "v_0bd737781f004ffe9b7f6ebe5bc3991d");
    
### Build template based query

    BqSelect rows = bq.select("alltabledata", "mytable")
                .useCache(true)
                .withPageSize(1000)
                .withPriority("INTERACTIVE")
                .build();

### Iterate paginated results (lazy loading)

    for (BqSelect.Row row : rows) {
            .....
        }

