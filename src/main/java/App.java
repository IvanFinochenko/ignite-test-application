import ignite.IgniteApplication;
import ignite.IgniteSourceServiseImpl;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
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

        try (Ignite ignite = Ignition.start()) {
            IgniteSourceServiseImpl igniteSourceServise =
                    new IgniteSourceServiseImpl(ignite, sourceService, parameters);

            igniteSourceServise.createCaches();
            igniteSourceServise.insertIntoCaches();
            new IgniteApplication(ignite, parameters).start();
        }
    }
}