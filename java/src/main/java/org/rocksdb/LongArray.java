// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class LongArray {
  public long[] buffer;
  public int length;

  public LongArray() { }
  public LongArray(long[] data) {
    buffer = data;
    length = data.length;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (o == null || !(o instanceof LongArray))
      return false;
    LongArray other = (LongArray) o;

    if (other.length != this.length)
      return false;

    for (int i = 0; i < this.length; ++i)
      if (other.buffer[i] != this.buffer[i])
        return false;

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    // same as the hashcode of the equivalent List<Long>
    for (int i = 0; i < length; ++i) {
      long e = buffer[i];

      hashCode = 31 * hashCode + (int)(e ^ (e >>> 32));
    }

    return hashCode;
  }

  private native static void initFieldIDs();

  static {
    RocksDB.loadLibrary();
    initFieldIDs();
  }
}
