/**
* Filename    : SyncServerBase.java
* Author      : Jack
* Create time : 2015-4-15 下午2:22:51
* Description :
*/
package com.cndw.asyncsync;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyncServerBase
{
  protected Log logger = LogFactory.getLog(DataSyncServer.class);

  private static AtomicBoolean dbErr = new AtomicBoolean(false);

  public static boolean isStopOnDBErr() {
    return dbErr.get();
  }

  public static void setStopOnDBErr(boolean stopOnDBErr) {
    dbErr.set(stopOnDBErr);
  }

  public static boolean stopCAS(boolean expect, boolean update) {
    return dbErr.compareAndSet(expect, update);
  }

  public static boolean isDbErr() {
    return dbErr.get();
  }
}
