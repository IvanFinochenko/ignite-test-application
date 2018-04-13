import org.junit.Test;

import java.sql.SQLException;

public class AppTest {
    @Test
    public void testApp() throws SQLException, ClassNotFoundException {
        String[] args = new String[1];
        args[0] = "2018-04-13";

        //It works
        App.main(args);
    }
}