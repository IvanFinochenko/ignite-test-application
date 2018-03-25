package rdbms;

import entity.Call;
import entity.CarWash;
import entity.Subscriber;

import java.time.LocalDateTime;
import java.util.List;

public interface SourceService {
    public List<Call> getCalls(LocalDateTime dateFrom, LocalDateTime dateUpTo);

    public List<Subscriber> getSubscribers();

    public List<CarWash> getCarWashes();
}
