package system;

import java.time.LocalDate;

public class Parameters {
    private static volatile Parameters instance;
    public final LocalDate today;

    private Parameters(String[] args) {
        if (args.length == 0)
            today = LocalDate.now();
        else
            today = LocalDate.parse(args[0]);
    }

    public static Parameters getInstance(String[] args) {
        Parameters localInstance = instance;
        if (localInstance == null) {
            synchronized (Parameters.class) {
                localInstance = instance;
                if (localInstance == null) {
                    instance = localInstance = new Parameters(args);
                }
            }
        }
        return localInstance;
    }
}
