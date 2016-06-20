package com.reactivetechnologies.csvloader;

public interface JobAllocator {
	void allocate();

  void clean() throws Exception;
  void load();
  int getloadCount();
}
