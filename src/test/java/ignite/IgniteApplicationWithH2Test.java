package ignite;

import org.junit.Before;
import rdbms.SourceServiceH2Impl;
import system.Parameters;

import java.sql.SQLException;

public class IgniteApplicationWithH2Test extends IgniteApplicationTest {
    @Before
    @Override
    public void setup() throws SQLException, ClassNotFoundException {
        String[] args = new String[1];
        args[0] = "2018-03-25";

        parameters = Parameters.getInstance(args);
        sourceService = new SourceServiceH2Impl();
    }
}
