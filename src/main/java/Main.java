import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Scheduler<String> scheduler = new Scheduler<>();
        scheduler.start();

        log.info("Started on " + LocalDateTime.now());

        // запустим задачу через 10 секунд после начала работы
        CompletableFuture<String> result = scheduler.schedule(LocalDateTime.now().plus(10, ChronoUnit.SECONDS), () -> {
            log.info("start 1");
            return LocalDateTime.now().toString();
        });
        LocalDateTime plus5 = LocalDateTime.now().plus(5, ChronoUnit.SECONDS);

        // Запустим 3 задачи в одно и то же время, через 5 секунд от начала работы.
        // Они должны выполниться в том же порядке, в котором добавлялись.
        CompletableFuture<String> result2 = scheduler.schedule(plus5, () -> {
            log.info("start 2");
            return LocalDateTime.now().toString();
        });
        CompletableFuture<String> result3 = scheduler.schedule(plus5, () -> {
            log.info("start 3");
            return LocalDateTime.now().toString();
        });
        CompletableFuture<String> result4 = scheduler.schedule(plus5, () -> {
            log.info("start 4");
            return LocalDateTime.now().toString();
        });

        // Наконец, запустим задачу прямо сейчас. Она должна выполниться сразу, до отложенных
        CompletableFuture<String> result5 = scheduler.schedule(LocalDateTime.now(), () -> {
            log.info("start 5");
            return LocalDateTime.now().toString();
        });
        // сначала дождемся задач, запланированных через 5 секунд
        log.info("finished 2 at " + result2.join());
        log.info("finished 3 at " + result3.join());
        log.info("finished 4 at " + result4.join());
        // эта задача должна была быть выполнена сразу, результат уже доступен, join не должен блокировать поток
        log.info("finished 5 at " + result5.join());
        // эта задача самая поздняя
        log.info("finished 1 at " + result.join());

        scheduler.stop();
    }
}
