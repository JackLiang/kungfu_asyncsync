/**
* Filename    : BatchSyncServer.java
* Author      : Jack
* Create time : 2015-4-15 下午2:24:32
* Description :
*/
package com.cndw.asyncsync.batch;

import com.cndw.asyncsync.SimpleThreadFactory;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Deprecated
public class BatchSyncServer
{
  private Log logger = LogFactory.getLog(getClass());

  private static Map<Class<?>, BatchSyncServer> instanceMap = new HashMap();
  private static BatchSyncHandler syncHandler;
  private ConcurrentLinkedQueue<BatchSyncObject> syncQueue = new ConcurrentLinkedQueue();
  private ExecutorService executor;
  private SyncProcess syncProcess;
  private int step = 50;

  private int speed = 60000;

  private boolean serverStop = false;

  public BatchSyncServer(Class<? extends BatchSyncObject> clazz)
  {
    this.syncProcess = new SyncProcess();
  }

  @Deprecated
  public static BatchSyncServer getInstance(Class<? extends BatchSyncObject> clazz) {
    if (!instanceMap.containsKey(clazz))
      synchronized (BatchSyncServer.class) {
        if (!instanceMap.containsKey(clazz)) {
          BatchSyncServer tmp = new BatchSyncServer(clazz);
          instanceMap.put(clazz, tmp);
        }
      }
    return (BatchSyncServer)instanceMap.get(clazz);
  }

  public static void setHandler(BatchSyncHandler batchSyncHandler) {
    syncHandler = batchSyncHandler;
  }

  public void commitSync(BatchSyncObject data) {
    this.syncQueue.add(data);
  }

  public synchronized void setSpeedAndStep(int speed, int step) {
    speed = speed <= 0 ? 100 : speed;
    step = (step <= 0) || (step >= 100) ? 10 : step;
    this.speed = speed;
    this.step = step;
    if (this.executor == null) {
      this.executor = Executors.newFixedThreadPool(step, new SimpleThreadFactory("log Async"));
    }
    if (this.syncProcess != null) {
      this.syncProcess.notifySpeed();
      if (!this.syncProcess.isAlive()) {
        this.syncProcess.start();
      }
    }
    this.logger.info("log async sync start  in the pool ");
  }

  public class SyncProcess extends Thread {
    private int millisWait = 200;

    public SyncProcess() {
      setName("log Async Sync ");
    }

    public void notifySpeed() {
      this.millisWait = (60000 / (BatchSyncServer.this.speed / BatchSyncServer.this.step));
      BatchSyncServer.this.logger.info("log async sync interval " + this.millisWait + " millisecond");
    }

    public void run()
    {
      while (true)
        try
        {
          initTask();

          if (BatchSyncServer.this.serverStop) {
            if (BatchSyncServer.this.syncQueue.isEmpty()) {
              break;
            }
          }
          else
            Thread.sleep(this.millisWait);
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
    }

    private void initTask()
    {
      final List logBatchObjects = new ArrayList();
      for (int i = 0; (i < BatchSyncServer.this.step) && (!BatchSyncServer.this.syncQueue.isEmpty()); i++) {
        BatchSyncObject logData = (BatchSyncObject)BatchSyncServer.this.syncQueue.remove();
        logBatchObjects.add(logData);
      }

      if (!logBatchObjects.isEmpty())
      {
        System.out.println(2);
        Runnable r = new Runnable()
        {
          public void run() {
            BatchSyncServer.syncHandler.save(logBatchObjects);
          }
        };
        BatchSyncServer.this.executor.execute(r);
      }
    }
  }
}
