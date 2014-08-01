package org.reactivestreams.tck;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.support.Optional;

public class TestEnvironment {
  public static final int TEST_BUFFER_SIZE = 16;

  private final long defaultTimeoutMillis;

  private CopyOnWriteArrayList<Throwable> asyncErrors = new CopyOnWriteArrayList<Throwable>();

  /**
   * Tests must specify the timeout for expected outcome of asynchronous
   * interactions. Longer timeout does not invalidate the correctness of
   * the implementation, but can in some cases result in longer time to
   * run the tests.
   *
   * @param defaultTimeoutMillis default timeout to be used in all expect* methods
   */
  public TestEnvironment(long defaultTimeoutMillis) {
    this.defaultTimeoutMillis = defaultTimeoutMillis;
  }

  // keeping method around
  public long defaultTimeoutMillis() { return defaultTimeoutMillis; }

  // don't use the name `fail` as it would collide with other `fail` definitions like the one in scalatest's traits
  public void flop(String msg) {
    try {
      fail(msg);
    } catch (Throwable t) {
      asyncErrors.add(t);
      throw new RuntimeException(t);
    }
  }

  // keeps passed in throwable as asyncError instead of creating a new AssertionError
  public void flop(Throwable thr, String msg) {
    try {
      fail(msg);
    } catch (Throwable t) {
      asyncErrors.add(thr);
      throw new RuntimeException(t);
    }
  }


  public <T extends Throwable> Throwable expectThrowingOf(Class<T> clazz, String errorMsg, Runnable block) throws Throwable {
    try {
      block.run();
      flop(errorMsg);
    } catch (Throwable e) {
      if (clazz.isInstance(e)) {
        // ok
        return e;
      } else if (org.reactivestreams.tck.support.NonFatal.apply(e)) {
        flop(errorMsg + " but " + e);
      } else {
        throw e;
      }
    }

    // make compiler happy
    return null;
  }

  public <T extends Throwable> void expectThrowingOfWithMessage(Class<T> clazz, String requiredMessagePart, Runnable block) throws Throwable {
    Throwable err = expectThrowingOf(clazz, String.format("Expected %s to be thrown", clazz), block);
    String message = err.getMessage();
    assertTrue(message.contains(requiredMessagePart),
               String.format("Got expected exception %s but missing message part [%s], was: %s", err, requiredMessagePart, message));
  }

  public <T> void subscribe(Publisher<T> pub, TestSubscriber<T> sub) throws InterruptedException {
    subscribe(pub, sub, defaultTimeoutMillis);
  }

  public <T> void subscribe(Publisher<T> pub, TestSubscriber<T> sub, long timeoutMillis) throws InterruptedException {
      pub.subscribe(sub);
      sub.subscription.expectCompletion(timeoutMillis, String.format("Could not subscribe %s to Publisher %s", sub, pub));
      verifyNoAsyncErrors();
    }


  public <T> ManualSubscriber<T> newManualSubscriber(Publisher<T> pub) throws InterruptedException {
    return newManualSubscriber(pub, defaultTimeoutMillis());
  }

  public <T> ManualSubscriber<T> newManualSubscriber(Publisher<T> pub, long timeoutMillis) throws InterruptedException {
    ManualSubscriberWithSubscriptionSupport<T> sub = new ManualSubscriberWithSubscriptionSupport<T>(this);
    subscribe(pub, sub, timeoutMillis);
    return sub;
  }

  public void clearAsyncErrors() {
    asyncErrors.clear();
  }

  public void verifyNoAsyncErrors() {
    for (Throwable e : asyncErrors) {
      if (e instanceof AssertionError) throw (AssertionError) e;
      else fail("Async error during test execution: " + e);
    }
  }

  // ---- classes ----

  public static class ManualSubscriberWithSubscriptionSupport<T> extends ManualSubscriber<T> {

    public ManualSubscriberWithSubscriptionSupport(TestEnvironment env) {
      super(env);
    }

    public void onNext(T element) {
      if (subscription.isCompleted()) {
        super.onNext(element);
      } else {
        env.flop("Subscriber::onNext(" + element + ") called before Subscriber::onSubscribe");
      }
    }

    public void onComplete() {
      if (subscription.isCompleted()) {
        super.onComplete();
      } else {
        env.flop("Subscriber::onComplete() called before Subscriber::onSubscribe");
      }
    }

    public void onSubscribe(Subscription s) {
      if (!subscription.isCompleted()) {
        subscription.complete(s);
      } else {
        env.flop("Subscriber::onSubscribe called on an already-subscribed Subscriber");
      }
    }

    public void onError(Throwable cause) {
      if (subscription.isCompleted()) {
        super.onError(cause);
      } else {
        env.flop(cause, "Subscriber::onError(" + cause + ") called before Subscriber::onSubscribe");
      }
    }
  }

  public static class TestSubscriber<T> implements Subscriber<T> {
    volatile Promise<Subscription> subscription;

    protected final TestEnvironment env;

    public TestSubscriber(TestEnvironment env) {
      this.env = env;
      subscription = new Promise<Subscription>(env);
    }

    @Override
    public void onError(Throwable cause)  {
      env.flop(cause, String.format("Unexpected Subscriber::onError(%s)", cause));
    }

    @Override
    public void onComplete() {
      env.flop("Unexpected Subscriber::onComplete()");
    }

    @Override
    public void onNext(T element) {
      env.flop(String.format("Unexpected Subscriber::onNext(%s)", element));
    }

    public void onSubscribe(Subscription subscription) {
      env.flop(String.format("Unexpected Subscriber::onSubscribe(%s)", subscription));
    }

    public void cancel() {
      if (subscription.isCompleted()) {
        subscription.value().cancel();
        subscription = new Promise<Subscription>(env);
      } else env.flop("Cannot cancel a subscription before having received it");
    }
  }

  public static class ManualSubscriber<T> extends TestSubscriber<T> {
    Receptacle<T> received = new Receptacle<T>(env);

    public ManualSubscriber(TestEnvironment env) {
      super(env);
    }

    @Override
    public void onNext(T element) {
      received.add(element);
    }

    @Override
    public void onComplete() {
      received.complete();
    }

    public void request(long elements) {
      subscription.value().request(elements);
    }

    public T requestNextElement() throws InterruptedException {
      return requestNextElement(env.defaultTimeoutMillis());
    }

    public T requestNextElement(long timeoutMillis) throws InterruptedException {
      return requestNextElement(timeoutMillis, "Did not receive expected element");
    }

    public T requestNextElement(String errorMsg) throws InterruptedException {
      return requestNextElement(env.defaultTimeoutMillis(), errorMsg);
    }

    public T requestNextElement(long timeoutMillis, String errorMsg) throws InterruptedException {
      request(1);
      return nextElement(timeoutMillis, errorMsg);
    }

    public Optional<T> requestNextElementOrEndOfStream(String errorMsg) throws InterruptedException {
      return requestNextElementOrEndOfStream(env.defaultTimeoutMillis(), errorMsg);
    }

    public Optional<T> requestNextElementOrEndOfStream(long timeoutMillis) throws InterruptedException {
      return requestNextElementOrEndOfStream(timeoutMillis, "Did not receive expected stream completion");
    }

    public Optional<T> requestNextElementOrEndOfStream(long timeoutMillis, String errorMsg) throws InterruptedException {
      request(1);
      return nextElementOrEndOfStream(timeoutMillis, errorMsg);
    }

    public void requestEndOfStream() throws InterruptedException {
      requestEndOfStream(env.defaultTimeoutMillis(), "Did not receive expected stream completion");
    }

    public void requestEndOfStream(long timeoutMillis) throws InterruptedException {
      requestEndOfStream(timeoutMillis, "Did not receive expected stream completion");
    }

    public void requestEndOfStream(String errorMsg) throws InterruptedException {
      requestEndOfStream(env.defaultTimeoutMillis(), errorMsg);
    }

    public void requestEndOfStream(long timeoutMillis, String errorMsg) throws InterruptedException {
      request(1);
      expectCompletion(timeoutMillis, errorMsg);
    }

    public List<T> requestNextElements(long elements) throws InterruptedException {
      request(elements);
      return nextElements(elements, env.defaultTimeoutMillis());
    }

    public List<T> requestNextElements(long elements, long timeoutMillis) throws InterruptedException {
      request(elements);
      return nextElements(elements, timeoutMillis, String.format("Did not receive %d expected elements", elements));
    }

    public List<T> requestNextElements(long elements, long timeoutMillis, String errorMsg) throws InterruptedException {
      request(elements);
      return nextElements(elements, timeoutMillis, errorMsg);
    }

    public T nextElement() throws InterruptedException {
      return nextElement(env.defaultTimeoutMillis());
    }

    public T nextElement(long timeoutMillis) throws InterruptedException {
      return nextElement(timeoutMillis, "Did not receive expected element");
    }

    public T nextElement(String errorMsg) throws InterruptedException {
      return nextElement(env.defaultTimeoutMillis(), errorMsg);
    }

    public T nextElement(long timeoutMillis, String errorMsg) throws InterruptedException {
      return received.next(timeoutMillis, errorMsg);
    }

    public Optional<T> nextElementOrEndOfStream() throws InterruptedException {
      return nextElementOrEndOfStream(env.defaultTimeoutMillis(), "Did not receive expected stream completion");
    }

    public Optional<T> nextElementOrEndOfStream(long timeoutMillis) throws InterruptedException {
      return nextElementOrEndOfStream(timeoutMillis, "Did not receive expected stream completion");
    }

    public Optional<T> nextElementOrEndOfStream(long timeoutMillis, String errorMsg) throws InterruptedException {
      return received.nextOrEndOfStream(timeoutMillis, errorMsg);
    }

    public List<T> nextElements(long elements) throws InterruptedException {
      return nextElements(elements, env.defaultTimeoutMillis(), "Did not receive expected element or completion");
    }

    public List<T> nextElements(long elements, String errorMsg) throws InterruptedException {
      return nextElements(elements, env.defaultTimeoutMillis(), errorMsg);
    }

    public List<T> nextElements(long elements, long timeoutMillis) throws InterruptedException {
      return nextElements(elements, timeoutMillis, "Did not receive expected element or completion");
    }

    public List<T> nextElements(long elements, long timeoutMillis, String errorMsg) throws InterruptedException {
      return received.nextN(elements, timeoutMillis, errorMsg);
    }

    public void expectNext(T expected) throws InterruptedException {
      expectNext(expected, env.defaultTimeoutMillis());
    }

    public void expectNext(T expected, long timeoutMillis) throws InterruptedException {
      T received = nextElement(timeoutMillis, "Did not receive expected element on downstream");
      if (!received.equals(expected)) {
        env.flop(String.format("Expected element %s on downstream but received %s", expected, received));
      }
    }

    public void expectCompletion() throws InterruptedException {
      expectCompletion(env.defaultTimeoutMillis(), "Did not receive expected stream completion");
    }

    public void expectCompletion(long timeoutMillis) throws InterruptedException {
      expectCompletion(timeoutMillis, "Did not receive expected stream completion");
    }

    public void expectCompletion(String errorMsg) throws InterruptedException {
      expectCompletion(env.defaultTimeoutMillis(), errorMsg);
    }

    public void expectCompletion(long timeoutMillis, String errorMsg) throws InterruptedException {
      received.expectCompletion(timeoutMillis, errorMsg);
    }

    public <E extends Throwable> void expectErrorWithMessage(Class<E> expected, String requiredMessagePart) throws Exception {
      E err = expectError(expected);
      String message = err.getMessage();
      assertTrue(message.contains(requiredMessagePart),
                 String.format("Got expected exception %s but missing message part [%s], was: %s", err, expected, requiredMessagePart));
    }

    public <E extends Throwable> E expectError(Class<E> expected) throws Exception {
      return expectError(expected, env.defaultTimeoutMillis(), "Expected onError");
    }

    public <E extends Throwable> E expectError(Class<E> expected, String errorMsg) throws Exception {
      return expectError(expected, env.defaultTimeoutMillis(), errorMsg);
    }

    public <E extends Throwable> E expectError(Class<E> expected, long timeoutMillis, String errorMsg) throws Exception {
      return received.expectError(expected, timeoutMillis, errorMsg);
    }

    public void expectNone() throws InterruptedException {
      expectNone(env.defaultTimeoutMillis());
    }

    public void expectNone(String errMsgPrefix) throws InterruptedException {
      received.expectNone(env.defaultTimeoutMillis(), errMsgPrefix);
    }

    public void expectNone(long withinMillis) throws InterruptedException {
      received.expectNone(withinMillis, "Did not expect an element but got ");
    }

  }

  public static class ManualPublisher<T> implements Publisher<T> {
    protected final TestEnvironment env;

    Optional<Subscriber<T>> subscriber = Optional.empty();
    Receptacle<Long> requests;
    Latch cancelled;

    public ManualPublisher(TestEnvironment env) {
      this.env = env;
      requests = new Receptacle<Long>(env);
      cancelled = new Latch(env);
    }

    @Override
    public void subscribe(Subscriber<T> s) {
      if (subscriber.isEmpty()) {
        subscriber = Optional.of(s);

        Subscription subs = new Subscription() {
          @Override
          public void request(long elements) {
            requests.add(elements);
          }

          @Override
          public void cancel() {
            cancelled.close();
          }
        };
        s.onSubscribe(subs);

      } else {
        env.flop("TestPublisher doesn't support more than one Subscriber");
      }
    }

    public void sendNext(T element) {
      if (subscriber.isDefined()) subscriber.get().onNext(element);
      else env.flop("Cannot sendNext before subscriber subscription");
    }

    public void sendCompletion() {
      if (subscriber.isDefined()) subscriber.get().onComplete();
      else env.flop("Cannot sendCompletion before subscriber subscription");
    }

    public void sendError(Throwable cause) {
      if (subscriber.isDefined()) subscriber.get().onError(cause);
      else env.flop("Cannot sendError before subscriber subscription");
    }

    public long nextRequest() throws InterruptedException {
      return nextRequest(env.defaultTimeoutMillis());
    }

    public long nextRequest(long timeoutMillis) throws InterruptedException {
      return requests.next(timeoutMillis, "Did not receive expected `request` call");
    }

    public long expectRequest() throws InterruptedException {
      return expectRequest(env.defaultTimeoutMillis());
    }

    public long expectRequest(long timeoutMillis) throws InterruptedException {
      long requested = nextRequest(timeoutMillis);
      if (requested <= 0) {
        env.flop(String.format("Requests cannot be zero or negative but received request(%s)", requested));
        return 0; // keep compiler happy
      } else
        return requested;
    }

    public void expectExactRequest(long expected) throws InterruptedException {
      expectExactRequest(expected, env.defaultTimeoutMillis());
    }

    public void expectExactRequest(long expected, long timeoutMillis) throws InterruptedException {
      long requested = expectRequest(timeoutMillis);
      if (requested != expected)
        env.flop(String.format("Received `request(%d)` on upstream but expected `request(%d)`", requested, expected));
    }

    public void expectNoRequest() throws InterruptedException {
      expectNoRequest(env.defaultTimeoutMillis());
    }

    public void expectNoRequest(long timeoutMillis) throws InterruptedException {
      requests.expectNone(timeoutMillis, "Received an unexpected call to: request");
    }

    public void expectCancelling() throws InterruptedException {
      expectCancelling(env.defaultTimeoutMillis());
    }

    public void expectCancelling(long timeoutMillis) throws InterruptedException {
      cancelled.expectClose(timeoutMillis, "Did not receive expected cancelling of upstream subscription");
    }
  }

  /** like a CountDownLatch, but resettable and with some convenience methods */
  public static class Latch {
    private final TestEnvironment env;
    volatile private CountDownLatch countDownLatch = new CountDownLatch(1);

    public Latch(TestEnvironment env) {
      this.env = env;
    }

    public void reOpen() {
      countDownLatch = new CountDownLatch(1);
    }

    public boolean isClosed() {
      return countDownLatch.getCount() == 0;
    }

    public void close() {
      countDownLatch.countDown();
    }

    public void assertClosed(String openErrorMsg) {
      if (!isClosed()) {
        env.flop(openErrorMsg);
      }
    }

    public void assertOpen(String closedErrorMsg) {
      if (isClosed()) {
        env.flop(closedErrorMsg);
      }
    }

    public void expectClose(long timeoutMillis, String notClosedErrorMsg) throws InterruptedException {
      countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
      if (countDownLatch.getCount() > 0) {
        env.flop(String.format("%s within %d ms", notClosedErrorMsg, timeoutMillis));
      }
    }
  }

  // simple promise for *one* value, which cannot be reset
  public static class Promise<T> {
    private final TestEnvironment env;

    public Promise(TestEnvironment env) {
      this.env = env;
    }

    private ArrayBlockingQueue<T> abq = new ArrayBlockingQueue<T>(1);
    volatile private T _value = null;

    public T value() {
      if (isCompleted()) {
        return _value;
      } else {
        env.flop("Cannot access promise value before completion");
        return null;
      }
    }

    public boolean isCompleted() {
      return _value != null;
    }

    public void complete(T value) {
      abq.add(value);
    }

    public void assertCompleted(String errorMsg) {
      if(!isCompleted())
        env.flop(errorMsg);
    }

    public void assertUncompleted(String errorMsg) {
    if(isCompleted())
      env.flop(errorMsg);
    }

    public void expectCompletion(long timeoutMillis, String errorMsg) throws InterruptedException {
      if (!isCompleted()) {
        T val = abq.poll(timeoutMillis, TimeUnit.MILLISECONDS);

        if (val == null) {
          env.flop(String.format("%s within %d ms", errorMsg, timeoutMillis));
        } else {
          _value = val;
        }
      }
    }
  }

   // a "Promise" for multiple values, which also supports "end-of-stream reached"
  public static class Receptacle<T> {
    final int QUEUE_SIZE = 2 * TEST_BUFFER_SIZE;
     private final TestEnvironment env;

     private ArrayBlockingQueue<Optional<T>> abq = new ArrayBlockingQueue<Optional<T>>(QUEUE_SIZE);

     Receptacle(TestEnvironment env) {
       this.env = env;
     }

     public void add(T value) {
      abq.add(Optional.of(value));
    }

    public void complete() {
      abq.add(Optional.<T>empty());
    }

    public T next(long timeoutMillis, String errorMsg) throws InterruptedException {
      Optional<T> value = abq.poll(timeoutMillis, TimeUnit.MILLISECONDS);

      if (value == null) {
        env.flop(String.format("%s within %d ms", errorMsg, timeoutMillis));
      } else if (value.isDefined()) {
        return value.get();
      } else {
        env.flop("Expected element but got end-of-stream");
      }

      return null; // keep compiler happy
    }

    public Optional<T> nextOrEndOfStream(long timeoutMillis, String errorMsg) throws InterruptedException {
      Optional<T> value = abq.poll(timeoutMillis, TimeUnit.MILLISECONDS);

      if (value == null) {
        env.flop(String.format("%s within %d ms", errorMsg, timeoutMillis));
      }

      return value;
    }

    public List<T> nextN(long elements, long timeoutMillis, String errorMsg) throws InterruptedException {
      List<T> result = new LinkedList<T>();
      long remaining = elements;
      while (remaining > 0) {
        result.add(next(timeoutMillis, errorMsg)); // TODO: fix error messages showing wrong timeout info
        remaining--;
      }

      return result;
    }


    public void expectCompletion(long timeoutMillis, String errorMsg) throws InterruptedException {
      Optional<T> value = abq.poll(timeoutMillis, TimeUnit.MILLISECONDS);

      if (value == null) {
        env.flop(String.format("%s within %d ms", errorMsg, timeoutMillis));
      } else if (value.isDefined()) {
        env.flop("Expected end-of-stream but got " + value.get());
      } // else, ok
    }

     public <E extends Throwable> E expectError(Class<E> clazz, long timeoutMillis, String errorMsg) throws Exception {
       Thread.sleep(timeoutMillis);

       if (env.asyncErrors.isEmpty()) {
         env.flop(String.format("%s within %d ms", errorMsg, timeoutMillis));
       } else {
         // ok, there was an expected error
         Throwable thrown = env.asyncErrors.remove(0);

         if (thrown != null && clazz.isInstance(thrown)) {
           return (E) thrown;
         } else if (thrown != null) {
           env.flop(String.format("%s within %d ms; Got %s but expected %s",
                                  errorMsg, timeoutMillis, thrown.getClass().getCanonicalName(), clazz.getCanonicalName()));
         } else {
           env.flop(String.format("%s within %d ms; Got `null` but expected %s",
                                  errorMsg, timeoutMillis, clazz.getCanonicalName()));
         }
       }

       // make compiler happy
       return null;
     }

     public void expectError(long timeoutMillis, String errorMsg) throws Exception {
       Thread.sleep(timeoutMillis);

       if (env.asyncErrors.isEmpty()) {
         env.flop(String.format("%s within %d ms", errorMsg, timeoutMillis));
       } else {
         // ok, there was an expected error
         env.asyncErrors.remove(0);
       }
     }

    void expectNone(long withinMillis, String errorMsgPrefix) throws InterruptedException {
      Thread.sleep(withinMillis);
      Optional<T> value = abq.poll();

      if (value == null) {
        // ok
      } else if (value.isDefined()) {
        env.flop(errorMsgPrefix + value.get());
      } else {
        env.flop("Expected no element but got end-of-stream");
      }
    }
  }
}

