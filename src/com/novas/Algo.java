package com.novas;

import java.io.IOException;
import java.io.InterruptedIOException;

public interface Algo {
      public void run(String username,long timestamp) throws IOException, ClassNotFoundException, InterruptedException;
}
