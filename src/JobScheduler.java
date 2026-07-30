import java.util.Map;
import java.util.concurrent.*;

public class JobScheduler {

    private final DelayQueue<Job> queue = new DelayQueue<>();

    private final ThreadPoolExecutor workerPool;

    private final Thread dispatcherThread;
    private final Thread timeoutThread;

    private final ConcurrentHashMap<String, JobStatus> jobStatus = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<?>> runningJobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> startTimeMap = new ConcurrentHashMap<>();

    /**
     * Initializes the scheduler.
     * Creates the worker thread pool and starts
     * the dispatcher and timeout monitor threads.
     */
    public JobScheduler() {

        this.workerPool = new ThreadPoolExecutor(
                50,
                100,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = new Thread(new Dispatcher(), "dispatcher");
        timeoutThread = new Thread(new TimeoutMonitor(), "timeout");

        dispatcherThread.start();
        timeoutThread.start();
    }

    /**
     * Schedules a new job for execution.
     *
     * @param id Unique job id
     * @param task Task to execute
     * @param delay Delay before execution (milliseconds)
     * @param retries Number of retry attempts if the job fails
     */
    public void submitJob(String id, Runnable task, long delay, int retries) {

        Job job = new Job(
                id,
                task,
                System.currentTimeMillis() + delay,
                retries
        );

        jobStatus.put(id, JobStatus.SCHEDULED);
        queue.offer(job);
    }

    /**
     * Continuously waits for scheduled jobs.
     * When a job's delay expires, submits it
     * to the worker thread pool.
     */
    private class Dispatcher implements Runnable {

        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {

                try {
                    Job job = queue.take();

                    jobStatus.put(job.getId(), JobStatus.RUNNING);
                    Future<?> future = workerPool.submit(() -> execute(job));
                    runningJobs.put(job.getId(), future);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Executes a job.
     * Marks it as SUCCESS, FAILED, CANCELLED,
     * or reschedules it if retries are available.
     *
     * @param job Job to execute
     */
    private void execute(Job job) {

        startTimeMap.put(job.getId(), System.currentTimeMillis());

        try {

            job.getTask().run();

            if (Thread.currentThread().isInterrupted()) {
                jobStatus.put(job.getId(), JobStatus.CANCELLED);
                return;
            }

            jobStatus.put(job.getId(), JobStatus.SUCCESS);

        } catch (Exception e) {

            if (job.canRetry()) {

                job.decrementRetry();
                job.setExecutionTime(System.currentTimeMillis() + 1000);
                queue.offer(job);

                jobStatus.put(job.getId(), JobStatus.SCHEDULED);

            } else {

                jobStatus.put(job.getId(), JobStatus.FAILED);
            }

        } finally {

            runningJobs.remove(job.getId());
            startTimeMap.remove(job.getId());
        }
    }

    /**
     * Continuously checks running jobs.
     * Cancels any job that runs longer than
     * the configured timeout (5 seconds).
     */
    private class TimeoutMonitor implements Runnable {

        @Override
        public void run() {

            while (!Thread.currentThread().isInterrupted()) {

                try {

                    for (Map.Entry<String, Future<?>> entry : runningJobs.entrySet()) {

                        String jobId = entry.getKey();
                        Future<?> future = entry.getValue();
                        Long start = startTimeMap.get(jobId);

                        if (start == null) continue;

                        long runningTime = System.currentTimeMillis() - start;

                        if (!future.isDone() && runningTime > 5000) {

                            future.cancel(true);
                            jobStatus.put(jobId, JobStatus.CANCELLED);
                        }
                    }

                    Thread.sleep(1000);

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /**
     * Prints the final execution summary.
     * Shows how many jobs succeeded,
     * failed, and were cancelled.
     */
    public void printStats() {

        long success = jobStatus.values().stream()
                .filter(s -> s == JobStatus.SUCCESS)
                .count();

        long failed = jobStatus.values().stream()
                .filter(s -> s == JobStatus.FAILED)
                .count();

        long cancelled = jobStatus.values().stream()
                .filter(s -> s == JobStatus.CANCELLED)
                .count();

        System.out.println("Success   : " + success);
        System.out.println("Failure   : " + failed);
        System.out.println("Cancelled : " + cancelled);
    }

    /**
     * Stops the scheduler gracefully.
     * Interrupts background threads and
     * waits for worker threads to finish.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void shutDown() throws InterruptedException {

        dispatcherThread.interrupt();
        timeoutThread.interrupt();

        workerPool.shutdown();
        workerPool.awaitTermination(30, TimeUnit.SECONDS);
    }
}
