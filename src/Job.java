import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Job implements Delayed {

    private final String id;
    private final Runnable task;

    private long scheduledTime;
    private int remainingRetries;

    public Job(String id, Runnable task, long scheduledTime, int retries) {
        this.id = id;
        this.task = task;
        this.scheduledTime = scheduledTime;
        this.remainingRetries = retries;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = scheduledTime - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        Job other = (Job) o;
        return Long.compare(this.scheduledTime, other.scheduledTime);
    }

    public String getId() {
        return id;
    }

    public Runnable getTask() {
        return task;
    }

    public boolean canRetry() {
        return remainingRetries > 0;
    }

    public void decrementRetry() {
        remainingRetries--;
    }

    public void setExecutionTime(long time) {
        this.scheduledTime = time;
    }
}