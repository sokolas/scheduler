import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * На вход поступают пары (LocalDateTime, Callable).
 * Нужно реализовать систему, которая будет выполнять Callable для каждого пришедшего события в указанном LocalDateTime,
 * либо как можно скорее в случае если система перегружена и не успевает все выполнять (имеет беклог).
 * Задачи должны выполняться в порядке согласно значению LocalDateTime либо в порядке прихода события для равных LocalDateTime.
 * События могут приходить в произвольном порядке и добавление новых пар (LocalDateTime, Callable) может вызываться из разных потоков.
 *<br>
 * Данная реализация не предполагает возможности перезапуска сервиса после остановки.
 *
 * @param <T> тип результата
 */
public class Scheduler<T> {
    // счетчик запланированных задач, чтобы сортировать их в порядке поступления при одинаковом времени
    private static AtomicLong taskCounter = new AtomicLong();

    // отложенные результаты выполнения задач
    private ConcurrentHashMap<Long, CompletableFuture<T>> results = new ConcurrentHashMap<>();

    // поток-планировщик, перекладывает задачи из запланированных в очередь на обработку, когда приходит их время
    private Thread schedulingThread;

    // поток-обработчик, выполняет задачи из очереди на выполнение по мере их поступления
    private Thread workerThread;

    private volatile boolean stopped = false;
    private volatile boolean started = false;

    // очередь запланированных задач, приоретизируется по времени выполнения, а если время одинаковое - в порядке поступления
    private PriorityBlockingQueue<Task<T>> scheduledTasks = new PriorityBlockingQueue<>();

    // очередь задач, для которых подошло время выполнения
    private BlockingQueue<Task<T>> runnableTasks = new LinkedBlockingQueue<>();

    public Scheduler() {
        schedulingThread = new Thread(() -> {
            while (!stopped) {
                Task<T> task = scheduledTasks.peek();
                if (task != null) {
                    long delay = LocalDateTime.now().until(task.getTime(), ChronoUnit.NANOS);
                    if (delay <= 0) {   // если пора выполнять задачу, то переложим ее в очередь готовых к выполнению
                        try {
                            Task<T> taskToRun = scheduledTasks.take();
                            // тут может оказаться задача, которую добавили после peek, но если она попала в голову
                            // очереди, то ее время уже пришло
                            runnableTasks.add(taskToRun);
                        } catch (InterruptedException e) {
                            // ничего страшного, продолжим цикл и проверим, надо ли останавливаться или нет
                        }
                    } else {    // если до ближайшей задачи еще есть время, то ждем до тех пор, пока оно не наступит, или пока поток не разбудят
                        LockSupport.parkNanos(delay);
                    }
                } else {    // если задач нет, то ждем без таймаута
                    LockSupport.park();
                }
            }
        });
        schedulingThread.setDaemon(true);

        workerThread = new Thread(() -> {
            while (!stopped) {
                try {
                    Task<T> task = runnableTasks.take();
                    CompletableFuture<T> result = results.remove(task.getSeqNum());
                    try {
                        result.complete(task.getCallable().call());
                    } catch (Throwable t) {
                        result.completeExceptionally(t);
                    }
                } catch (InterruptedException ie) {
                    // скорее всего, тред остановили извне, просто пойдем дальше по циклу и выйдем нормально
                }
            }
        });
        workerThread.setDaemon(true);
    }

    /**
     * Запуск сервиса. Сервис - "одноразовый" и не предполагает повторного запуска.
     */
    public void start() {
        if (stopped) {
            throw new IllegalStateException("Cannot restart");
        }
        if (started) {
            throw new IllegalStateException("Already started");
        }
        started = true;
        workerThread.start();
        schedulingThread.start();
    }

    /**
     * Остановка сервиса.
     * При этом очередь заданий не очищается
     */
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Not started yet");
        }
        if (stopped) {
            throw new IllegalStateException("Already stopped");
        }
        stopped = true;
        workerThread.interrupt();
        schedulingThread.interrupt();
    }

    /**
     * Отправка задания на выполнение
     *
     * @param time время, в которое должно выполниться задание
     * @param callable задание на выполнение
     * @return отложенный результат выполнения задания
     */
    public CompletableFuture<T> schedule(LocalDateTime time, Callable<T> callable) {
        if (stopped) {
            throw new IllegalStateException("Stopped");
        }
        Objects.requireNonNull(time, "time can not be null");
        Objects.requireNonNull(callable, "callable can not be null");

        long taskNumber = taskCounter.incrementAndGet();
        Task<T> task = new Task<>(taskNumber, time, callable);
        CompletableFuture<T> result = new CompletableFuture<>();
        results.put(taskNumber, result);
        scheduledTasks.add(task);
        Task<T> nextTask = scheduledTasks.peek();
        if (nextTask != null) {
            // разбудим поток-планировщик, чтобы он скорректировал время до ближайшей задачи
            LockSupport.unpark(schedulingThread);
        }
        return result;
    }

    /**
     * Внутренний класс, представляющий задачу на выполнение с временем, в которое она по возможности должна выполниться,
     * и порядковым номером.
     * @param <T> тип возвращаемого задачей результата
     */

    public static class Task<T> implements Comparable<Task> {

        private final long seqNum;
        private final LocalDateTime time;
        private final Callable<T> callable;

        public Task(long seqNum, LocalDateTime time, Callable<T> callable) {
            this.seqNum = seqNum;
            this.time = time;
            this.callable = callable;
        }

        public long getSeqNum() {
            return seqNum;
        }

        public LocalDateTime getTime() {
            return time;
        }

        public Callable<T> getCallable() {
            return callable;
        }

        @Override
        public int compareTo(Task other) {
            int dateComparison = this.time.compareTo(other.time);
            if (dateComparison == 0) {
                // PriorityBlockingQueue не гарантирует порядок элементов с одинаковым приоритетом,
                // т.е. просто LocalDateTime для сортировки недостаточно
                return Long.compare(this.seqNum, other.seqNum);
            } else {
                return dateComparison;
            }
        }
    }
}
