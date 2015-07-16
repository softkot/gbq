import org.junit.Test;
import ru.softlynx.gbq.BqContext;
import ru.softlynx.gbq.BqSelect;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.HashMap;

public class LibraryTest {

    @Test
    public void bqContext() throws URISyntaxException, GeneralSecurityException, IOException {
        BqContext bq = new BqContext.Builder()
                .withProjectId("alco-control")
                .withServiceAccount("272433143339-rs14caqptempvr50reqn5mc6b41mb655@developer.gserviceaccount.com")
                .withP12KeyURL(this.getClass().getResource("bq-key.p12"))
                .withTemplateLibrary("tests.vm")
                .build();

        bq.put("DATASET", "v_0bd737781f004ffe9b7f6ebe5bc3991d");
        BqSelect q = bq.select("alltabledata", "refs")
                .useCache(true)
                .withPageSize(1000)
                .withPriority(BqSelect.PRIO_INTERACTIVE)
                .build();
        q.getStatus();
        assert q.isCompleted();
        HashMap<String, Integer> columns = q.getColumnIndexes();
        for (BqSelect.Row row : q) {
            row.toString();
            break;
        }
    }

}
