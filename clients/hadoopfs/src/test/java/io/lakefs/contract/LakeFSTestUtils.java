package io.lakefs.contract;

import io.lakefs.LakeFSFileSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.internal.AssumptionViolatedException;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

public class LakeFSTestUtils {

  public static final String NULL_RESULT = "(null)";

  public static LakeFSFileSystem createTestFileSystem(Configuration conf) throws IOException {
    String fsname = conf.getTrimmed(TestLakeFSFileSystemContract.TEST_FS_LAKEFS_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals("lakefs");
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException(
          "No test filesystem in " + TestLakeFSFileSystemContract.TEST_FS_LAKEFS_NAME);
    }
    LakeFSFileSystem fs1 = new LakeFSFileSystem();
    //enable purging in tests
    conf.setBoolean(Constants.PURGE_EXISTING_MULTIPART, true);
    conf.setInt(Constants.PURGE_EXISTING_MULTIPART_AGE, 0);
    fs1.initialize(testURI, conf);
    return fs1;
  }


  /**
   * Intercept an exception; throw an {@code AssertionError} if one not raised.
   * The caught exception is rethrown if it is of the wrong class or
   * does not contain the text defined in {@code contained}.
   * <p>
   * Example: expect deleting a nonexistent file to raise a
   * {@code FileNotFoundException}.
   * <pre>
   * FileNotFoundException ioe = intercept(FileNotFoundException.class,
   *   () -> {
   *     filesystem.delete(new Path("/missing"), false);
   *   });
   * </pre>
   *
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param eval expression to eval
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   */
  @SuppressWarnings("unchecked")
  public static <T, E extends Throwable> E intercept(
          Class<E> clazz,
          Callable<T> eval)
          throws Exception {
    return intercept(clazz,
            null,
            "Expected a " + clazz.getName() + " to be thrown," +
                    " but got the result: ",
            eval);
  }

  /**
   * Variant of {@link #intercept(Class, Callable)} to simplify void
   * invocations.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param eval expression to eval
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   */
  @SuppressWarnings("unchecked")
  public static <E extends Throwable> E intercept(
          Class<E> clazz,
          VoidCallable eval)
          throws Exception {
    try {
      eval.call();
      throw new AssertionError("Expected an exception of type " + clazz);
    } catch (Throwable e) {
      if (clazz.isAssignableFrom(e.getClass())) {
        return (E)e;
      }
      throw e;
    }
  }

  /**
   * Intercept an exception; throw an {@code AssertionError} if one not raised.
   * The caught exception is rethrown if it is of the wrong class or
   * does not contain the text defined in {@code contained}.
   * <p>
   * Example: expect deleting a nonexistent file to raise a
   * {@code FileNotFoundException} with the {@code toString()} value
   * containing the text {@code "missing"}.
   * <pre>
   * FileNotFoundException ioe = intercept(FileNotFoundException.class,
   *   "missing",
   *   () -> {
   *     filesystem.delete(new Path("/missing"), false);
   *   });
   * </pre>
   *
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param eval expression to eval
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type and contents
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   * @see GenericTestUtils#assertExceptionContains(String, Throwable)
   */
  public static <T, E extends Throwable> E intercept(
          Class<E> clazz,
          String contained,
          Callable<T> eval)
          throws Exception {
    E ex = intercept(clazz, eval);
    GenericTestUtils.assertExceptionContains(contained, ex);
    return ex;
  }

  /**
   * Intercept an exception; throw an {@code AssertionError} if one not raised.
   * The caught exception is rethrown if it is of the wrong class or
   * does not contain the text defined in {@code contained}.
   * <p>
   * Example: expect deleting a nonexistent file to raise a
   * {@code FileNotFoundException} with the {@code toString()} value
   * containing the text {@code "missing"}.
   * <pre>
   * FileNotFoundException ioe = intercept(FileNotFoundException.class,
   *   "missing",
   *   "path should not be found",
   *   () -> {
   *     filesystem.delete(new Path("/missing"), false);
   *   });
   * </pre>
   *
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param message any message tho include in exception/log messages
   * @param eval expression to eval
   * @param <T> return type of expression
   * @param <E> exception class
   * @return the caught exception if it was of the expected type and contents
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   * The error includes the {@code toString()} value of the result, if this
   * can be determined.
   * @see GenericTestUtils#assertExceptionContains(String, Throwable)
   */
  public static <T, E extends Throwable> E intercept(
          Class<E> clazz,
          String contained,
          String message,
          Callable<T> eval)
          throws Exception {
    E ex;
    try {
      T result = eval.call();
      throw new AssertionError(message + ": " + robustToString(result));
    } catch (Throwable e) {
      if (!clazz.isAssignableFrom(e.getClass())) {
        throw e;
      } else {
        ex = (E) e;
      }
    }
    GenericTestUtils.assertExceptionContains(contained, ex);
    return ex;
  }

  /**
   * Variant of {@link #intercept(Class, Callable)} to simplify void
   * invocations.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param eval expression to eval
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   */
  public static <E extends Throwable> E intercept(
          Class<E> clazz,
          String contained,
          VoidCallable eval)
          throws Exception {
    return intercept(clazz, contained,
            "Expecting " + clazz.getName()
                    + (contained != null? (" with text " + contained) : "")
                    + " but got ",
            () -> {
              eval.call();
              return "void";
            });
  }

  /**
   * Variant of {@link #intercept(Class, Callable)} to simplify void
   * invocations.
   * @param clazz class of exception; the raised exception must be this class
   * <i>or a subclass</i>.
   * @param contained string which must be in the {@code toString()} value
   * of the exception
   * @param message any message tho include in exception/log messages
   * @param eval expression to eval
   * @param <E> exception class
   * @return the caught exception if it was of the expected type
   * @throws Exception any other exception raised
   * @throws AssertionError if the evaluation call didn't raise an exception.
   */
  public static <E extends Throwable> E intercept(
          Class<E> clazz,
          String contained,
          String message,
          VoidCallable eval)
          throws Exception {
    return intercept(clazz, contained, message,
            () -> {
              eval.call();
              return "void";
            });
  }

  /**
   * Robust string converter for exception messages; if the {@code toString()}
   * method throws an exception then that exception is caught and logged,
   * then a simple string of the classname logged.
   * This stops a {@code toString()} failure hiding underlying problems.
   * @param o object to stringify
   * @return a string for exception messages
   */
  private static String robustToString(Object o) {
    if (o == null) {
      return NULL_RESULT;
    } else {
      if (o instanceof String) {
        return '"' + (String)o + '"';
      }
      try {
        return o.toString();
      } catch (Exception e) {
//        LOG.info("Exception calling toString()", e);
        return o.getClass().toString();
      }
    }
  }

  /**
   * A simple interface for lambdas, which returns nothing; this exists
   * to simplify lambda tests on operations with no return value.
   */
  @FunctionalInterface
  public interface VoidCallable {
    void call() throws Exception;
  }

  /**
   * Bridge class to make {@link VoidCallable} something to use in anything
   * which takes an {@link Callable}.
   */
  public static class VoidCaller implements Callable<Void> {
    private final VoidCallable callback;

    public VoidCaller(VoidCallable callback) {
      this.callback = callback;
    }

    @Override
    public Void call() throws Exception {
      callback.call();
      return null;
    }
  }
}
