package rdbms;

import org.junit.Before;

import java.sql.SQLException;

public class SourceServiceH2ImplTest extends SourceServiceExampleTest {
    @Before
    @Override
    public void setup() throws SQLException, ClassNotFoundException {
        sourceService = new SourceServiceH2Impl();
    }
}
