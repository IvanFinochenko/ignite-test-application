import ignite.IgniteApplication;
import rdbms.SourceService;
import rdbms.SourceServiceExampleImpl;
import rdbms.SourceServiceH2Impl;
import system.Parameters;

import java.sql.SQLException;

public class App {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Parameters parameters = Parameters.getInstance(args);
        //SourceService sourceService = new SourceServiceExampleImpl();
        SourceService sourceService = new SourceServiceH2Impl();

        new IgniteApplication(parameters, sourceService).start();
    }
}