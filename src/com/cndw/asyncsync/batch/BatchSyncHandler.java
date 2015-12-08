/**
* Filename    : BatchSyncHandler.java
* Author      : Jack
* Create time : 2015-4-15 下午2:23:45
* Description :
*/
package com.cndw.asyncsync.batch;

import java.util.List;

public abstract interface BatchSyncHandler
{
  public abstract void save(List<BatchSyncObject> paramList);
}
