# Google BigQuery java wrapper 

## Simplify access to BigQuery with power of Apache velocity templated SQL and paginated results.

Use it in a next simple steps.

### Create SQL template resource file (sql.vm)

    #macro(alltabledata $table)
    SELECT * FROM [${DATASET}.$table] $filter
    #end

### Create context object to connect BigQuery with specified credentials

    BqContext context = new BqContext.Builder()
                .withProjectId("my-big-query-project")
                .withServiceAccount("272433143339-rs14caqptempvr50reqn5mc6b41mb655@developer.gserviceaccount.com")
                .withP12KeyURL(this.getClass().getResource("service-account-key.p12"))
                .withTemplateLibrary("sql.vm")
                .build();

### Populate context with some global template variables

    context.put("DATASET", "v_0bd737781f004ffe9b7f6ebe5bc3991d");
    
### Build template based query with local template variables and macro parameters

    BqSelect rows = context.select("alltabledata", "mytable")
                .useCache(true)
                .withPageSize(1000)
                .withPriority("INTERACTIVE")
                .put("filter","where myvalue > 10")
                .build();

### Iterate paginated results (lazy loading)

    for (BqSelect.Row row : rows) {
            .....
        }

