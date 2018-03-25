package rdbms;

import entity.Call;
import entity.CarWash;
import entity.Subscriber;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SourceServiceExampleImpl implements SourceService {
    @Override
    public List<Call> getCalls(LocalDateTime dateFrom, LocalDateTime dateUpTo) {
        LocalDateTime dt = LocalDateTime.now();

        List<Call> list =  new ArrayList<>(Arrays.asList(
                new Call(88002553535L, 88002553534L, 110, dt),
                new Call(89202550008L, 88002550004L, 120, dt),
                new Call(89202550005L, 89202550000L, 60, dt.plusDays(1)),
                new Call(89202550005L, 88002550001L, 59, dt.plusDays(10)),
                new Call(89202550007L, 88002550003L, 12, dt.plusDays(20)),
                new Call(89202550009L, 88002550005L, 130, dt.minusDays(1)),
                new Call(89202550005L, 88002550002L, 90, dt.minusDays(10))
        ));

        return filterCalls(list, dateFrom, dateUpTo);
    }

    @Override
    public List<Subscriber> getSubscribers() {
        LocalDate dateMinus2Y = LocalDate.now().minusYears(2);

        return new ArrayList<>(Arrays.asList(
                new Subscriber(88002553535L, "Ivanov Ivan Ivanovich",
                        "21, Moscow District, Voronezh, Russia", dateMinus2Y.plusMonths(5)),
                new Subscriber(89202550008L, "Pakhomov Alexander Sergeevich",
                        "21, Moscow District, Voronezh, Russia", dateMinus2Y),
                new Subscriber(89202550005L, "Petrov Petr Petrovich",
                        "21, Moscow District, Voronezh, Russia", dateMinus2Y),
                new Subscriber(89202550007L, "Someboby elsovich",
                        "21, Moscow District, Voronezh, Russia", dateMinus2Y.plusMonths(5)),
                new Subscriber(89202550009L, "Someboby elsovich 2",
                        "21, Moscow District, Voronezh, Russia", dateMinus2Y),
                new Subscriber(89202550011L, "Someboby elsovich 3",
                        "21, Moscow District, Voronezh, Russia", dateMinus2Y.plusYears(2))

        ));
    }

    @Override
    public List<CarWash> getCarWashes() {

        return new ArrayList<>(Arrays.asList(
                new CarWash(88002553534L, "Bumerang",
                        "1, Moscow District, Voronezh, Russia", 0),
                new CarWash(88002550004L, "VimpelcomWash 1",
                        "22, Moscow District, Voronezh, Russia", 1),
                new CarWash(89202550000L, "VimpelcomWash 2",
                        "23, Moscow District, Voronezh, Russia", 1),
                new CarWash(88002550003L, "VimpelcomWash 3",
                        "24, Moscow District, Voronezh, Russia", 1),
                new CarWash(88002550002L, "Washik",
                        "25, Moscow District, Voronezh, Russia", 0)

        ));
    }

    private List<Call> filterCalls(List<Call> call, LocalDateTime dateFrom, LocalDateTime dateUpTo) {
        return call.stream().filter((c) ->
                c.getStartTime().isBefore(dateUpTo) && c.getStartTime().isAfter(dateFrom))
                .collect(Collectors.toList());
    }
}
