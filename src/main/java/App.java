import ignite.IgniteApplication;
import rdbms.SourceService;
import rdbms.SourceServiceExampleImpl;
import system.Parameters;

import java.sql.SQLException;

public class App {
    public static void main(String[] args) throws SQLException {
        Parameters parameters = Parameters.getInstance(args);
        SourceService sourceService = new SourceServiceExampleImpl();

        new IgniteApplication(parameters, sourceService).start();
    }
}