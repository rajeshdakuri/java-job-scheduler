public class Main {

    public static void main(String[] args) throws InterruptedException {

        JobScheduler scheduler = new JobScheduler();

        System.out.println("===== STARTING JOB TEST =====");

        // 1. SIMPLE SUCCESS JOB
        scheduler.submitJob(
                "JOB-1",
                () -> {
                    try {
                        System.out.println("JOB-1 started");
                        Thread.sleep(2000);
                        System.out.println("JOB-1 finished");
                    } catch (InterruptedException e) {
                        System.out.println("JOB-1 interrupted");
                        Thread.currentThread().interrupt();
                    }
                },
                1000,
                2
        );

        // 2. TIMEOUT JOB (WILL BE CANCELLED)
        scheduler.submitJob(
                "JOB-2",
                () -> {
                    try {
                        System.out.println("JOB-2 started (long task)");
                        Thread.sleep(8000);
                        System.out.println("JOB-2 finished");
                    } catch (InterruptedException e) {
                        System.out.println("JOB-2 interrupted ❌");
                        Thread.currentThread().interrupt();
                    }
                },
                1000,
                2
        );

        // 3. FAILING JOB (NO RETRY SUCCESS)
        scheduler.submitJob(
                "JOB-3",
                () -> {
                    System.out.println("JOB-3 started");
                    throw new RuntimeException("JOB-3 failed intentionally");
                },
                1000,
                1
        );

        // WAIT FOR ALL JOBS TO FINISH
        Thread.sleep(12000);

        System.out.println("\n===== FINAL STATS =====");
        scheduler.printStats();
        scheduler.shutDown();
        System.out.println("===== END =====");
    }
}