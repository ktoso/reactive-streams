package org.reactivestreams.tck.support;

import static org.testng.Assert.fail;

/**
 * Provides assertions to validate the TCK tests themselves,
 * with the goal of guaranteeing proper error messages when an implementation does not pass a given TCK test.
 *
 * "Who watches the watchmen?" -- Juvenal
 */
public class TCKVerificationSupport {

  // INTERNAL ASSERTION METHODS //

  /**
   * Runs given code block and expects it to fail with an "Expected onError" failure.
   * Use this method to validate that TCK tests fail with meaningful errors instead of NullPointerExceptions etc.
   *
   * @param run encapsulates test case which we expect to fail
   * @param msgPart the exception failing the test (inside the run parameter) must contain this message part in one of it's causes
   */
  public void requireTestFailure(ThrowingRunnable run, String msgPart) {
    try {
      run.run();
    } catch (Throwable throwable) {
      if (findDeepErrorMessage(throwable, msgPart)) {
        return;
      } else {
        throw new RuntimeException(
          "Expected TCK to fail with '... " + msgPart + " ...', yet `" + throwable.getClass().getName() + "(" + throwable.getMessage() + ")` was thrown " +
          "and test would fail with not useful error message!", throwable);
      }
    }
    throw new RuntimeException("Expected TCK to fail with '... " + msgPart + " ...', yet no exception was thrown and test would pass unexpectedly!");
  }

  /**
   * Looks for expected error message prefix inside of causes of thrown throwable.
   *
   * @return true if one of the causes indeed contains expected error, false otherwise
   */
  public boolean findDeepErrorMessage(Throwable throwable, String msgPart) {
    return findDeepErrorMessage(throwable, msgPart, 5);
  }

  private boolean findDeepErrorMessage(Throwable throwable, String msgPart, int depth) {
    if (throwable instanceof NullPointerException) {
      fail("" + NullPointerException.class.getName() + " was thrown, definitely not a helpful error!", throwable);
      return false;
    } else if (throwable == null || depth == 0) {
      return false;
    } else {
      final String message = throwable.getMessage();
      return (message != null && message.contains(msgPart)) || findDeepErrorMessage(throwable.getCause(), msgPart, depth - 1);
    }
  }

  public static interface ThrowingRunnable {
    void run() throws Throwable;
  }
}
