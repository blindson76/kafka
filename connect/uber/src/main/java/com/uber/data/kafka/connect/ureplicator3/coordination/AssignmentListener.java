package com.uber.data.kafka.connect.ureplicator3.coordination;

public interface AssignmentListener {

  /**
   * Invoked when the assignment for the task has been updated. Long-running or blocking
   * operations should <strong>not</strong> be performed inside this method
   */
  void onUpdate();

}
