/**
* Filename    : DataSyncServer.java
* Author      : Jack
* Create time : 2015-4-15 下午2:20:44
* Description :
*/
package com.cndw.asyncsync;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;

public class DataSyncServer extends SyncServerBase
{
  private ConcurrentLinkedQueue<SyncObject> syncQueue = new ConcurrentLinkedQueue();
  private ExecutorService executor;
  private SyncProcess syncProcess;
  private static DataSyncServer instance = null;

  private int poolSize = 10;

  private int minuteSpeed = 3000;

  private boolean serverStop = false;

  private boolean executorStop = false;

  private DataSyncServer() {
    this.syncProcess = new SyncProcess();
  }

  public static DataSyncServer getInstance() {
    if (instance == null) {
      synchronized (DataSyncServer.class) {
        if (instance == null) {
          instance = new DataSyncServer();
        }
      }
    }
    return instance;
  }

  public synchronized void setSpeedAndStep(int minuteSpeed, int poolSize)
  {
    minuteSpeed = minuteSpeed <= 0 ? 100 : minuteSpeed;
    poolSize = (poolSize <= 0) || (poolSize > 300) ? 10 : poolSize;

    if (this.executor == null) {
      this.executor = Executors.newFixedThreadPool(poolSize, new SimpleThreadFactory("Async"));
    } else if ((poolSize != this.poolSize) && 
      ((this.executor instanceof ThreadPoolExecutor))) {
      ((ThreadPoolExecutor)this.executor).setMaximumPoolSize(poolSize);
      this.logger.info("setMaximumPoolSize " + poolSize);
    }

    this.minuteSpeed = minuteSpeed;
    this.poolSize = poolSize;
    if (this.syncProcess != null) {
      this.syncProcess.notifySpeed();
      if (!this.syncProcess.isAlive())
        this.syncProcess.start();
    }
  }

  public void commitSync(SyncObject data)
  {
    if (data.stateCAS(false, true)) {
      this.syncQueue.add(data);
    }
    if (this.executorStop)
      this.logger.error("err commit  on serverStop " + data.getClass().getSimpleName());
  }

  public int size()
  {
    return this.syncQueue.size();
  }

  public int getMinuteSpeed() {
    return this.minuteSpeed;
  }

  public int getPoolSize() {
    return this.poolSize;
  }

  public void shutDown() {
    shutDown(300);
  }

  public void shutDown(int waitSconds) {
    shutDown(waitSconds, this.poolSize);
  }

  public void shutDown(int waitSconds, int maximumPoolSize) {
    this.logger.info("async sync shutdown ... ...");
    this.logger.info("syncQueue size " + size());
    this.serverStop = true;

    if (this.poolSize != maximumPoolSize) {
      this.poolSize = maximumPoolSize;
      if ((this.executor instanceof ThreadPoolExecutor)) {
        ((ThreadPoolExecutor)this.executor).setMaximumPoolSize(maximumPoolSize);
        this.logger.info("setMaximumPoolSize " + maximumPoolSize);
      }
    }
    try
    {
      if (this.executor != null) {
        this.logger.info("syncQueue on shutdown size " + size());
        if (waitSconds > 0) {
          this.executorStop = true;
          this.logger.info("executor shutdown " + waitSconds);
          try {
            this.syncProcess.interrupt();
          } catch (Exception e) {
            e.printStackTrace();
          }
          try {
            this.syncProcess.join();
          } catch (Exception e) {
            e.printStackTrace();
          }
          this.executor.shutdown();
          this.executor.awaitTermination(waitSconds, TimeUnit.SECONDS);
        }
      }
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }

    this.logger.info("async sync stop suc");
  }

  public class SyncProcess extends Thread {
    private int millisWait = 200;

    public SyncProcess() {
      setName("Async Sync ");
    }

    public void notifySpeed() {
      this.millisWait = (60000 / DataSyncServer.this.minuteSpeed * DataSyncServer.this.poolSize);
      DataSyncServer.this.logger.info("interval " + this.millisWait + " millisecond,minuteSpeed " + DataSyncServer.this.minuteSpeed + ",poolSize " + DataSyncServer.this.poolSize);
    }

    public void run()
    {
      while (true) {
        try
        {
          initTask();

          if (DataSyncServer.this.serverStop) {
            if (DataSyncServer.this.syncQueue.isEmpty()) {
              if (DataSyncServer.this.executorStop) {
                break;
              }
              Thread.sleep(this.millisWait);
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

      DataSyncServer.this.logger.info("SyncProcess stop ... ... ... ... ...  stop final ...");
    }

    private void initTask()
    {
      if (SyncServerBase.isDbErr()) {
        return;
      }

      for (int i = 0; (i < DataSyncServer.this.poolSize) && (!DataSyncServer.this.syncQueue.isEmpty()); i++) {
        final SyncObject syncData = (SyncObject)DataSyncServer.this.syncQueue.poll();
        if (syncData != null)
        {
          Runnable r = new Runnable()
          {
            public void run() {
              if (!syncData.stateCAS(true, false)) {
                DataSyncServer.this.logger.error("DataSyncService Object Status Error!,Expect true,But " + syncData.stateGet());
              }
              boolean suc = syncData.save();

              if ((!suc) && (syncData.retry() >= 0) && 
                (syncData.stateCAS(false, true))) {
                DataSyncServer.this.syncQueue.add(syncData);
                DataSyncServer.this.logger.info("Sync Failure " + syncData.getRetryCount() + " " + syncData.getClass().getSimpleName());
              }
            }
          };
          DataSyncServer.this.executor.execute(r);
        }
      }
    }
  }
}