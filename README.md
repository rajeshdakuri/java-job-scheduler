# Java Job Scheduler

A multithreaded job scheduler built in Java that supports delayed execution, retries, timeout handling, job cancellation, and concurrent execution using Java Concurrency APIs.

---

## Features

- Delayed job execution using `DelayQueue`
- Concurrent execution using `ThreadPoolExecutor`
- Automatic retry for failed jobs
- Timeout monitoring
- Job cancellation
- Thread-safe job tracking
- Job status management
- Graceful shutdown

---

## Technologies

- Java
- Java Concurrency
- DelayQueue
- ThreadPoolExecutor
- Future
- ConcurrentHashMap
- Runnable

---

## Architecture

```
                submitJob()
                     |
                     ▼
             DelayQueue<Job>
                     |
                     ▼
           Dispatcher Thread
                     |
                     ▼
          ThreadPoolExecutor
                     |
          +----------+----------+
          |                     |
          ▼                     ▼
    Execute Job          Timeout Monitor
          |                     |
          ▼                     ▼
   Update Job Status      Cancel Long Jobs
```

---

## Components

### Job

Represents a scheduled task.

Responsibilities:
- Stores job information
- Maintains execution time
- Maintains retry count
- Implements `Delayed`

---

### JobScheduler

Core scheduling engine.

Responsibilities:
- Accept jobs
- Dispatch jobs
- Execute jobs
- Retry failed jobs
- Track running jobs
- Monitor timeout
- Maintain job status

---

### Dispatcher

Continuously waits for jobs whose delay has expired and submits them to the worker thread pool.

---

### TimeoutMonitor

Monitors currently running jobs.

If a job runs longer than 5 seconds, it is cancelled automatically.

---

### JobStatus

Represents the lifecycle of a job.

- SCHEDULED
- RUNNING
- SUCCESS
- FAILED
- CANCELLED

---

## Project Flow

1. Submit a job
2. Store it in `DelayQueue`
3. Wait until scheduled time
4. Dispatcher picks the job
5. Worker thread executes it
6. On success → SUCCESS
7. On failure → Retry (if retries remain)
8. On timeout → CANCELLED

---

## Sample Output

```
===== STARTING JOB TEST =====

JOB-1 started
JOB-2 started (long task)
JOB-3 started

JOB-1 finished
JOB-2 interrupted

===== FINAL STATS =====

Success   : 1
Failure   : 1
Cancelled : 1
```

---

## Concurrency Concepts Used

- DelayQueue
- ThreadPoolExecutor
- Future
- Runnable
- ConcurrentHashMap
- Thread interruption
- Producer-Consumer pattern
- Graceful shutdown

---

## Learning Objectives

This project demonstrates practical usage of:

- Java Multithreading
- Concurrent Collections
- Scheduling Algorithms
- Thread Pool Management
- Retry Mechanism
- Timeout Handling
- Thread Interruption
- Concurrent Programming Best Practices
