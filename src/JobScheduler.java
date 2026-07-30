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

    public JobScheduler() {

        this.workerPool = new ThreadPoolExecutor(
                50,                          // Core pool size
                100,                         // Maximum pool size
                60,                          // Idle thread keep-alive time
                TimeUnit.SECONDS,            // Keep-alive time unit
                new LinkedBlockingQueue<>(10000), // Queue can hold 10,000 tasks
                new ThreadPoolExecutor.CallerRunsPolicy() // If queue is full and max threads reached, calling thread executes the task
        );

        dispatcherThread = new Thread(new Dispatcher(), "dispatcher");
        timeoutThread = new Thread(new TimeoutMonitor(), "timeout");

        dispatcherThread.start();
        timeoutThread.start();
    }

    // ---------------- SUBMIT ----------------

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

    // ---------------- DISPATCHER ----------------

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

    // ---------------- EXECUTION ----------------

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

    // ---------------- TIMEOUT ----------------

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

    // ---------------- FINAL STATS (NO BUGS EVER) ----------------

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

    // ---------------- SHUTDOWN ----------------

    public void shutDown() throws InterruptedException {

        dispatcherThread.interrupt();
        timeoutThread.interrupt();

        workerPool.shutdown();
        workerPool.awaitTermination(30, TimeUnit.SECONDS);
    }
}
