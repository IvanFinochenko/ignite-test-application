package rdbms;

import entity.Call;
import entity.CarWash;
import entity.Subscriber;

import java.time.LocalDateTime;
import java.util.List;

public class SourceServiceH2Impl implements SourceService {
    @Override
    public List<Call> getCalls(LocalDateTime dateFrom, LocalDateTime dateUpTo) {
        return null;
    }

    @Override
    public List<Subscriber> getSubscribers() {
        return null;
    }

    @Override
    public List<CarWash> getCarWashes() {
        return null;
    }
}
