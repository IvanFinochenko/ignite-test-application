import ignite.IgniteApplication;
import ignite.IgniteSourceServiceImpl;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import rdbms.SourceService;
import rdbms.SourceServiceH2Impl;
import system.Parameters;

import java.sql.SQLException;

public class App {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Parameters parameters = Parameters.getInstance(args);
        //SourceService sourceService = new SourceServiceExampleImpl();
        SourceService sourceService = new SourceServiceH2Impl();

        try (Ignite ignite = Ignition.start()) {
            IgniteSourceServiceImpl igniteSourceService =
                    new IgniteSourceServiceImpl(ignite, sourceService, parameters);

            igniteSourceService.createCachesAndInsert();
            new IgniteApplication(ignite, parameters).start();
        }
    }
}