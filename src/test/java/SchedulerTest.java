import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class SchedulerTest {
    @Test
    public void testErrorIllegalState() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        IllegalStateException exception0 = assertThrows(IllegalStateException.class, () -> s.stop());
        assertEquals("Not started yet", exception0.getMessage());
        s.start();
        IllegalStateException exception1 = assertThrows(IllegalStateException.class, () -> s.start());
        assertEquals("Already started", exception1.getMessage());
        s.stop();
        IllegalStateException exception2 = assertThrows(IllegalStateException.class, () -> s.stop());
        assertEquals("Already stopped", exception2.getMessage());
        IllegalStateException exception3 = assertThrows(IllegalStateException.class, () -> s.start());
        assertEquals("Cannot restart", exception3.getMessage());
        IllegalStateException exception4 = assertThrows(IllegalStateException.class,
                () -> s.schedule(LocalDateTime.now(), LocalDateTime::now));
        assertEquals("Stopped", exception4.getMessage());
    }

    @Test
    public void testErrorNullTask() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();
        NullPointerException exception1 = assertThrows(NullPointerException.class,
                () -> s.schedule(null, LocalDateTime::now));
        assertEquals("time can not be null", exception1.getMessage());
        NullPointerException exception2 = assertThrows(NullPointerException.class,
                () -> s.schedule(LocalDateTime.now(), null));
        assertEquals("callable can not be null", exception2.getMessage());

        s.stop();
    }

    @Test
    public void testRunNow() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();
        LocalDateTime now = LocalDateTime.now();
        CompletableFuture<LocalDateTime> result = s.schedule(now, LocalDateTime::now);
        LocalDateTime runTime = result.join();
        assertFalse(now.isAfter(runTime));
        // невозможно точно узнать, сколько времени потребуется, поэтому будем считать, что 100мс достаточно на выполнение
        assertTrue(now.plus(100, ChronoUnit.MILLIS).isAfter(runTime));
        s.stop();
    }

    @Test
    public void testRunWithException() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();

        LocalDateTime now = LocalDateTime.now();
        CompletableFuture<LocalDateTime> result1 = s.schedule(now.plus(50, ChronoUnit.MILLIS),
                () -> {
                    throw new IOException("io error");
                });
        CompletionException completionException = assertThrows(CompletionException.class, () -> result1.join());
        assertEquals(IOException.class, completionException.getCause().getClass());
        assertEquals("io error", completionException.getCause().getMessage());
        s.stop();
    }

    @Test
    public void testRunWithDifferentTimes() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();

        LocalDateTime now = LocalDateTime.now();
        CompletableFuture<LocalDateTime> result1 = s.schedule(now.plus(50, ChronoUnit.MILLIS), LocalDateTime::now);
        CompletableFuture<LocalDateTime> result2 = s.schedule(now.plus(100, ChronoUnit.MILLIS), LocalDateTime::now);
        LocalDateTime date1 = result1.join();
        LocalDateTime date2 = result2.join();
        assertTrue(now.plus(50, ChronoUnit.MILLIS).compareTo(date1) <= 0);
        assertTrue(now.plus(80, ChronoUnit.MILLIS).compareTo(date1) > 0);
        assertTrue(now.plus(100, ChronoUnit.MILLIS).compareTo(date2) <= 0);
        assertTrue(now.plus(130, ChronoUnit.MILLIS).compareTo(date2) > 0);

        s.stop();
    }

    @Test
    public void testRunWithDifferentTimes_reverse() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();

        LocalDateTime now = LocalDateTime.now();
        CompletableFuture<LocalDateTime> result2 = s.schedule(now.plus(100, ChronoUnit.MILLIS), LocalDateTime::now);
        CompletableFuture<LocalDateTime> result1 = s.schedule(now.plus(50, ChronoUnit.MILLIS), LocalDateTime::now);
        LocalDateTime date1 = result1.join();
        LocalDateTime date2 = result2.join();
        assertTrue(now.plus(50, ChronoUnit.MILLIS).compareTo(date1) <= 0);
        assertTrue(now.plus(80, ChronoUnit.MILLIS).compareTo(date1) > 0);
        assertTrue(now.plus(100, ChronoUnit.MILLIS).compareTo(date2) <= 0);
        assertTrue(now.plus(130, ChronoUnit.MILLIS).compareTo(date2) > 0);

        s.stop();
    }

    @Test
    public void testRunWithSameTime() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();

        AtomicInteger counter = new AtomicInteger(0);

        LocalDateTime now = LocalDateTime.now();
        CompletableFuture<LocalDateTime> result1 = s.schedule(now.plus(100, ChronoUnit.MILLIS),
                () -> {
                    LocalDateTime start = LocalDateTime.now();
                    counter.set(1);
                    return start;
                });
        CompletableFuture<LocalDateTime> result2 = s.schedule(now.plus(100, ChronoUnit.MILLIS),
                () -> {
                    LocalDateTime start = LocalDateTime.now();
                    counter.set(2);
                    return start;
                });
        LocalDateTime date1 = result1.join();
        LocalDateTime date2 = result2.join();
        assertTrue(now.plus(100, ChronoUnit.MILLIS).compareTo(date1) <= 0);
        assertTrue(now.plus(130, ChronoUnit.MILLIS).compareTo(date1) > 0);
        assertTrue(now.plus(100, ChronoUnit.MILLIS).compareTo(date2) <= 0);
        assertTrue(now.plus(130, ChronoUnit.MILLIS).compareTo(date2) > 0);
        // Не всегда дата выполнения 2й задачи будет больше 1й, потому что LocalDateTime.now() иногда возвращает
        // одинаковые значения. Будем определять, что выполнилось позже, по тому, какое значение counter установлено последним
        assertEquals(2, counter.get());

        s.stop();
    }

    @Test
    public void testRunLongTasks() {
        Scheduler<LocalDateTime> s = new Scheduler<>();
        s.start();

        LocalDateTime now = LocalDateTime.now();
        CompletableFuture<LocalDateTime> result1 = s.schedule(now,
                () -> {
                    LocalDateTime start = LocalDateTime.now();
                    Thread.sleep(200);
                    return start;
                });
        CompletableFuture<LocalDateTime> result2 = s.schedule(now,
                () -> {
                    LocalDateTime start = LocalDateTime.now();
                    Thread.sleep(200);
                    return start;
                });
        LocalDateTime date1 = result1.join();
        LocalDateTime date2 = result2.join();
        assertTrue(now.compareTo(date1) <= 0);
        assertTrue(now.plus(30, ChronoUnit.MILLIS).compareTo(date1) > 0);
        assertTrue(now.plus(200, ChronoUnit.MILLIS).compareTo(date2) <= 0);
        assertTrue(now.plus(230, ChronoUnit.MILLIS).compareTo(date2) > 0);

        s.stop();
    }

}
