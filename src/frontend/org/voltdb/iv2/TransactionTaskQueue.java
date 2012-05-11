package org.voltdb.iv2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;

public class TransactionTaskQueue
{
    final private static VoltLogger hostLog = new VoltLogger("HOST");

    final private Deque<TransactionTask> m_backlog =
        new ArrayDeque<TransactionTask>();
    final private SiteTaskerQueue m_taskQueue;
    private final ExecutorService es =
            Executors.newSingleThreadExecutor(CoreUtils.getThreadFactory("TransactionTaskQueue"));

    TransactionTaskQueue(SiteTaskerQueue queue)
    {
        m_taskQueue = queue;
    }

    private class Flusher implements Runnable
    {
        CountDownLatch m_taskDoneLatch;
        TransactionTaskQueue m_queue;

        Flusher(CountDownLatch doneLatch, TransactionTaskQueue queue)
        {
            m_taskDoneLatch = doneLatch;
            m_queue = queue;
        }

        @Override
        public void run()
        {
            try {
                m_taskDoneLatch.await();
                m_queue.flush();
            }
            catch (InterruptedException e) {
                // IZZY what to do here?
                e.printStackTrace();
            }
        }
    };

    /**
     * If necessary, stick this task in the backlog.
     * Many network threads may be racing to reach here, synchronize to
     * serialize queue order
     * @param task
     * @return true if this task was stored, false if not
     */
    synchronized boolean offer(TransactionTask task)
    {
        hostLog.debug("OFFERED:  " + task);
        boolean retval = false;
        // Single partitions never queue if empty
        // Multipartitions always queue
        // Fragments queue if they're not part of the queue head TXN ID
        // offer to SiteTaskerQueue if:
        // the queue was empty
        // the queue wasn't empty but the txn IDs matched
        if (!m_backlog.isEmpty()) {
            if (task.getMpTxnId() != m_backlog.getFirst().getMpTxnId())
            {
                if (!task.getTransactionState().isSinglePartition()) {
                    es.submit(new Flusher(task.getTransactionState().getDoneLatch(),
                                          this));
                }
                m_backlog.addLast(task);
                retval = true;
            }
            else {
                hostLog.debug("QUEUED:   " + task);
                m_taskQueue.offer(task);
            }
        }
        else {
            if (!task.getTransactionState().isSinglePartition()) {
                es.submit(new Flusher(task.getTransactionState().getDoneLatch(),
                                      this));
                m_backlog.addLast(task);
                retval = true;
            }
            hostLog.debug("QUEUED:   " + task);
            m_taskQueue.offer(task);
        }
        return retval;
    }

    /**
     * Try to offer as many runnable Tasks to the SiteTaskerQueue as possible.
     * Currently just blocks on the next uncompleted multipartition transaction
     * @return
     */
    synchronized int flush()
    {
        int offered = 0;
        // check to see if head is done
        // then offer until the next MP or FragTask
        if (!m_backlog.isEmpty()) {
            hostLog.debug("FLUSHED: " + m_backlog.getFirst().toString());
            if (m_backlog.getFirst().getTransactionState().getDoneLatch().getCount() == 0) {
                // remove the completed MP txn
                m_backlog.removeFirst();
                while (!m_backlog.isEmpty()) {
                    TransactionTask next = m_backlog.getFirst();
                    hostLog.debug("QUEUED:   " + next);
                    m_taskQueue.offer(next);
                    ++offered;
                    if (next.getTransactionState().isSinglePartition()) {
                        m_backlog.removeFirst();
                    }
                    else {
                        break;
                    }
                }
            }
        }
        return offered;
    }

    /**
     * How many Tasks are un-runnable?
     * @return
     */
    synchronized int size()
    {
        return m_backlog.size();
    }
}
