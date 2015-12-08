/**
* Filename    : SyncObject.java
* Author      : Jack
* Create time : 2015-4-15 下午2:22:30
* Description :
*/
package com.cndw.asyncsync;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SyncObject
{
  private transient AtomicBoolean syncOnQueue = new AtomicBoolean(false);

  private transient int syncCounts = 0;
  private transient long syncLastTime;
  public static final transient int sysRetryTimes = 3;
  private transient int retryCount = 3;

  public abstract boolean syncDB();

  public int getSyncCounts()
  {
    return this.syncCounts;
  }

  public long getSyncLastTime() {
    return this.syncLastTime;
  }

  public int getRetryCount() {
    return this.retryCount;
  }

  public boolean stateCAS(boolean expect, boolean update) {
    if ((expect) && (!update)) {
      this.syncLastTime = System.currentTimeMillis();
      this.syncCounts += 1;
      this.retryCount = 3;
    }
    return this.syncOnQueue.compareAndSet(expect, update);
  }

  public boolean stateGet() {
    return this.syncOnQueue.get();
  }

  public void commitSync()
  {
    DataSyncServer.getInstance().commitSync(this);
  }

  public int retry() {
    this.retryCount -= 1;
    return this.retryCount;
  }

  @Deprecated
  public void delaySync(int seconds)
  {
    commitSync();
  }

  public boolean save()
  {
    return syncDB();
  }

  public String info() {
    StringBuffer sb = new StringBuffer();
    sb.append(getClass().getSimpleName());
    sb.append(",syncCounts=").append(this.syncCounts);
    sb.append(",syncLastTime=").append(this.syncLastTime);
    sb.append(",syncOnQueue=").append(this.syncOnQueue.get());
    return sb.toString();
  }
}
