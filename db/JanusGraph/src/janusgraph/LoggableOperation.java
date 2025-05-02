package janusgraph;

import java.util.ArrayList;
import java.util.List;

public interface LoggableOperation extends Runnable {
    /**
     * get logs during the operation
     */
    List<String> getLogs();
}

