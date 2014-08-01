package org.reactivestreams.tck;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment.*;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.reactivestreams.tck.Annotations.*;
import static org.testng.Assert.assertTrue;

/**
 * Provides tests for verifying {@code Subscriber} and {@code Subscription}specification rules.
 *
 * @see org.reactivestreams.Subscriber
 * @see org.reactivestreams.Subscription
 */
public abstract class SubscriberVerification<T> {

  private final TestEnvironment env;

  protected SubscriberVerification(TestEnvironment env) {
    this.env = env;
  }

  /**
   * This is the main method you must implement in your test incarnation.
   * It must create a new Subscriber instance to be subjected to the testing logic.
   *
   * In order to be meaningfully testable your Subscriber must inform the given
   * `SubscriberProbe` of the respective events having been received.
   */
  public abstract Subscriber<T> createSubscriber(SubscriberProbe<T> probe);

  /**
   * Helper method required for generating test elements.
   * It must create a Publisher for a stream with exactly the given number of elements.
   * If `elements` is `Long.MAX_VALUE` the produced stream must be infinite.
   */
  public abstract Publisher<T> createHelperPublisher(long elements);

  ////////////////////// TEST ENV CLEANUP /////////////////////////////////////

  @BeforeMethod
  public void setUp() throws Exception {
    env.clearAsyncErrors();
  }

  ////////////////////// TEST SETUP VERIFICATION //////////////////////////////

  @Required @Test
  public void exerciseHappyPath() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(1);
        stage.puppet().triggerRequest(1);

        long receivedRequests = stage.expectRequest();

        stage.signalNext();
        stage.probe.expectNext(stage.lastT);

        stage.puppet().triggerRequest(1);
        if (receivedRequests == 1) {
          stage.expectRequest();
        }

        stage.signalNext();
        stage.probe.expectNext(stage.lastT);

        stage.puppet().signalCancel();
        stage.expectCancelling();

        stage.verifyNoAsyncErrors();
      }
    });
  }

  ////////////////////// SPEC RULE VERIFICATION ///////////////////////////////

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.1
  @Required @Test
  public void spec201_mustSignalDemandViaSubscriptionRequest() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(1);
        stage.expectRequest();

        stage.signalNext();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.2
  @NotVerified @Test
  public void spec202_shouldAsynchronouslyDispatch() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.3
  @NotVerified @Test
  public void spec203_mustNotCallMethodsOnSubscriptionOrPublisherInOnComplete() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.3
  @NotVerified @Test
  public void spec203_mustNotCallMethodsOnSubscriptionOrPublisherInOnError() throws Exception {
    // cannot be meaningfully tested, or can it?
    notVerified();
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.4
  @NotVerified @Test
  public void spec204_mustConsiderTheSubscriptionAsCancelledInAfterRecievingOnCompleteOrOnError() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.5
  @Required @Test
  public void spec205_mustCallSubscriptionCancelIfItAlreadyHasAnSubscriptionAndReceivesAnotherOnSubscribeSignal() throws Exception {
    new TestStage(env) {{
      // try to subscribe another time, if the subscriber calls `probe.registerOnSubscribe` the test will fail
      sub().onSubscribe(
          new Subscription() {
            public void request(long elements) {
              env.flop(String.format("Subscriber %s illegally called `subscription.request(%s)`", sub(), elements));
            }

            public void cancel() {
              env.flop(String.format("Subscriber %s illegally called `subscription.cancel()`", sub()));
            }
          });

      env.verifyNoAsyncErrors();
    }};
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.6
  @NotVerified @Test
  public void spec206_mustCallSubscriptionCancelIfItIsNoLongerValid() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.7
  @NotVerified @Test
  public void spec207_mustEnsureAllCallsOnItsSubscriptionTakePlaceFromTheSameThread() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
    // the same thread part of the clause can be verified but that is not very useful, or is it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.8
  @Required @Test
  public void spec208_mustBePreparedToReceiveOnNextSignalsAfterHavingCalledSubscriptionCancel() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(1);
        stage.puppet().signalCancel();

        stage.puppet().triggerRequest(1);
        stage.puppet().triggerRequest(1);

        stage.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.9
  @Required  @Test
  public void spec209_mustBePreparedToReceiveAnOnCompleteSignalWithPrecedingRequestCall() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(1);
        stage.sendCompletion();
        stage.probe.expectCompletion();

        stage.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.9
  @Required @Test
  public void spec209_mustBePreparedToReceiveAnOnCompleteSignalWithoutPrecedingRequestCall() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.sendCompletion();
        stage.probe.expectCompletion();

        stage.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.10
  @Required @Test
  public void spec210_mustBePreparedToReceiveAnOnErrorSignalWithPrecedingRequestCall() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(1);
        stage.puppet().triggerRequest(1);

        Exception ex = new RuntimeException("Test exception");
        stage.sendError(ex);
        stage.probe.expectError(ex);

        env.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.10
  @Required @Test
  public void spec210_mustBePreparedToReceiveAnOnErrorSignalWithoutPrecedingRequestCall() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        Exception ex = new RuntimeException("Test exception");
        stage.sendError(ex);
        stage.probe.expectError(ex);
        env.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.11
  @NotVerified @Test
  public void spec211_mustMakeSureThatAllCallsOnItsMethodsHappenBeforeTheProcessingOfTheRespectiveEvents() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.12
  @Required @Test
  public void spec212_mustNotCallOnSubscribeMoreThanOnceBasedOnObjectEquality() throws Throwable {
    subscriberTestWithoutSetup(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.pub = stage.createHelperPublisher(Long.MAX_VALUE);
        stage.tees = env.newManualSubscriber(stage.pub);
        stage.probe = stage.createSubscriberProbe();
        stage.subscribe(createSubscriber(stage.probe));
        stage.probe.expectCompletion(env.defaultTimeoutMillis(), String.format("Subscriber %s did not `registerOnSubscribe`", stage.sub()));
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.13
  @NotVerified @Test
  public void spec213_failingOnCompleteInvocation() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#2.14
  @NotVerified @Test
  public void spec214_failingOnErrorInvocation() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  ////////////////////// SUBSCRIPTION SPEC RULE VERIFICATION //////////////////

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.1
  @NotVerified @Test
  public void spec301_mustNotBeCalledOutsideSubscriberContext() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.2
  @Required @Test
  public void spec302_mustAllowSynchronousRequestCallsFromOnNextAndOnSubscribe() throws Throwable {
    subscriberTestWithoutSetup(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.pub = stage.createHelperPublisher(Long.MAX_VALUE);
        stage.tees = env.newManualSubscriber(stage.pub);
        stage.probe = stage.createSubscriberProbe();

        stage.subscribe(new ManualSubscriber<T>(env) {
          public void onSubscribe(Subscription subs) {
            this.subscription.complete(subs);

            subs.request(1);
            subs.request(1);
            subs.request(1);
          }

          public void onNext(T element) {
            Subscription subs = this.subscription.value();
            subs.request(1);
            subs.request(1);
            subs.request(1);
          }
        });

        env.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.3
  @NotVerified @Test
  public void spec303_mustNotAllowUnboundedRecursion() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
    // notice: could be tested if we knew who's responsibility it is to break the loop.
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.4
  @NotVerified @Test
  public void spec304_requestShouldNotPerformHeavyComputations() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.5
  @NotVerified @Test
  public void spec305_mustNotSynchronouslyPerformHeavyCompuatation() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.6
  @Required @Test
  public void spec306_afterSubscriptionIsCancelledRequestMustBeNops() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws Exception {
        stage.puppet().signalCancel();
        stage.expectCancelling();

        stage.puppet().triggerRequest(1);
        stage.puppet().triggerRequest(1);
        stage.puppet().triggerRequest(1);

        stage.probe.expectNone();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.7
  @Required @Test
  public void spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws Exception {
        stage.puppet().signalCancel();
        stage.expectCancelling();

        stage.puppet().signalCancel();

        stage.probe.expectNone();
        stage.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.8
  @Required @Test
  public void spec308_requestMustRegisterGivenNumberElementsToBeProduced() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(2);
        stage.probe.expectNext(stage.signalNext());
        stage.probe.expectNext(stage.signalNext());

        stage.probe.expectNone();
        stage.puppet().triggerRequest(3);
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.9
  @Required @Test
  public void spec309_callingRequestZeroMustThrow() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(final TestStage stage) throws Throwable {
        env.expectThrowingOfWithMessage(IllegalArgumentException.class, "3.9", new Runnable() {
          @Override
          public void run() {
            stage.puppet().triggerRequest(Long.MAX_VALUE);
          }
        });
        env.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.9
  @Required @Test
  public void spec309_callingRequestWithNegativeNumberMustThrow() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(final TestStage stage) throws Throwable {
        env.expectThrowingOfWithMessage(IllegalArgumentException.class, "3.9", new Runnable() {
          @Override
          public void run() {
            stage.puppet().triggerRequest(-1);
          }
        });
        env.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.10
  @NotVerified @Test
  public void spec310_requestMaySynchronouslyCallOnNextOnSubscriber() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.11
  @NotVerified @Test
  public void spec311_requestMaySynchronouslyCallOnCompleteOrOnError() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.12
  @Required @Test
  public void spec312_cancelMustRequestThePublisherToEventuallyStopSignaling() throws Throwable {
    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().signalCancel();
        stage.expectCancelling();
        stage.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.14
  @NotVerified @Test
  public void spec314_cancelMayCauseThePublisherToShutdownIfNoOtherSubscriptionExists() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.15
  @NotVerified @Test
  public void spec315_cancelMustNotThrowExceptionAndMustSignalOnError() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.16
  @NotVerified @Test
  public void spec316_requestMustNotThrowExceptionAndMustOnErrorTheSubscriber() throws Exception {
    notVerified(); // cannot be meaningfully tested, or can it?
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.17
  @Required @Test
  public void spec317_mustSupportAPendingElementCountUpToLongMaxValue() throws Throwable {
    // TODO please read into this one, not sure about semantics

    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(Long.MAX_VALUE);

          stage.probe.expectNext(stage.signalNext());

        // to avoid error messages during test harness shutdown
        stage.sendCompletion();
        stage.probe.expectCompletion();

        stage.verifyNoAsyncErrors();
      }
    });
  }

  // Verifies rule: https://github.com/reactive-streams/reactive-streams#3.17
  @Required @Test
  public void spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue() throws Throwable {
    // TODO please read into this one, not sure about semantics

    subscriberTest(new SubscriberTestRun() {
      @Override
      public void run(TestStage stage) throws InterruptedException {
        stage.puppet().triggerRequest(Long.MAX_VALUE - 1);
        stage.puppet().triggerRequest(Long.MAX_VALUE - 1);

        // cumulative pending > Long.MAX_VALUE
        stage.probe.expectErrorWithMessage(IllegalStateException.class, "3.17");
      }
    });
  }


  /////////////////////// ADDITIONAL "COROLLARY" TESTS ////////////////////////

  /////////////////////// TEST INFRASTRUCTURE /////////////////////////////////

  abstract class SubscriberTestRun {
    public abstract void run(TestStage stage) throws Throwable;
  }

  public void subscriberTest(SubscriberTestRun body) throws Throwable {
    TestStage stage = new TestStage(env, true);
    body.run(stage);
  }

  public void subscriberTestWithoutSetup(SubscriberTestRun body) throws Throwable {
    TestStage stage = new TestStage(env, false);
    body.run(stage);
  }

  public class TestStage extends ManualPublisher<T> {
    public Publisher<T> pub;
    public ManualSubscriber<T> tees; // gives us access to an infinite stream of T values
    public Probe probe;

    public T lastT = null;

    public TestStage(TestEnvironment env) throws InterruptedException {
      this(env, true);
    }

    public TestStage(TestEnvironment env, boolean runDefaultInit) throws InterruptedException {
      super(env);
      if (runDefaultInit) {
        pub = this.createHelperPublisher(Long.MAX_VALUE);
        tees = env.newManualSubscriber(pub);
        probe = new Probe();
        subscribe(createSubscriber(probe));
        probe.puppet.expectCompletion(env.defaultTimeoutMillis(), String.format("Subscriber %s did not `registerOnSubscribe`", sub()));
      }
    }

    public Subscriber<T> sub() {
      return subscriber.get();
    }

    public Publisher<T> createHelperPublisher(long elements) {
      return SubscriberVerification.this.createHelperPublisher(elements);
    }

    public Probe createSubscriberProbe() {
      return new Probe();
    }

    public SubscriberPuppet puppet() {
      return probe.puppet.value();
    }

    public T signalNext() throws InterruptedException {
      T element = nextT();
      sendNext(element);
      return element;
    }

    public T nextT() throws InterruptedException {
      lastT = tees.requestNextElement();
      return lastT;
    }

    public void verifyNoAsyncErrors() {
      env.verifyNoAsyncErrors();
    }

    public class Probe implements SubscriberProbe<T> {
      Promise<SubscriberPuppet> puppet = new Promise<SubscriberPuppet>(env);
      Receptacle<T> elements = new Receptacle<T>(env);
      Latch completed = new Latch(env);
      Promise<Throwable> error = new Promise<Throwable>(env);

      @Override
      public void registerOnSubscribe(SubscriberPuppet p) {
        if (!puppet.isCompleted()) {
          puppet.complete(p);
        } else {
          env.flop(String.format("Subscriber %s illegally accepted a second Subscription", sub()));
        }
      }

      @Override
      public void registerOnNext(T element) {
        elements.add(element);
      }

      @Override
      public void registerOnComplete() {
        completed.close();
      }

      @Override
      public void registerOnError(Throwable cause) {
        error.complete(cause);
      }

      public void expectNext(T expected) throws InterruptedException {
        expectNext(expected, env.defaultTimeoutMillis());
      }

      public void expectNext(T expected, long timeoutMillis) throws InterruptedException {
        T received = elements.next(timeoutMillis, String.format("Subscriber %s did not call `registerOnNext(%s)`", sub(), expected));
        if (!received.equals(expected)) {
          env.flop(String.format("Subscriber %s called `registerOnNext(%s)` rather than `registerOnNext(%s)`", sub(), received, expected));
        }
      }

      public void expectCompletion() throws InterruptedException {
        expectCompletion(env.defaultTimeoutMillis());
      }

      public void expectCompletion(long timeoutMillis) throws InterruptedException {
        expectCompletion(timeoutMillis, String.format("Subscriber %s did not call `registerOnComplete()`", sub()));
      }

      public void expectCompletion(long timeoutMillis, String msg) throws InterruptedException {
        completed.expectClose(timeoutMillis, msg);
      }

      public <E extends Throwable> void expectErrorWithMessage(Class<E> expected, String requiredMessagePart) throws InterruptedException {
        E err = expectError(expected);
        String message = err.getMessage();
        assertTrue(message.contains(requiredMessagePart),
                   String.format("Got expected exception %s but missing message [%s], was: %s", err, expected, requiredMessagePart));
      }
      public <E extends Throwable> E expectError(Class<E> expected) throws InterruptedException {
        return expectError(expected, env.defaultTimeoutMillis());
      }

      public <E extends Throwable> E expectError(Class<E> expected, long timeoutMillis) throws InterruptedException {
        error.expectCompletion(timeoutMillis, String.format("Subscriber %s did not call `registerOnError(%s)`", sub(), expected));
        if (expected.isInstance(error.value())) {
          return (E) error.value();
        } else {
          env.flop(String.format("Subscriber %s called `registerOnError(%s)` rather than `registerOnError(%s)`", sub(), error.value(), expected));

          // make compiler happy
          return null;
        }
      }

      public void expectError(Throwable expected) throws InterruptedException {
        expectError(expected, env.defaultTimeoutMillis());
      }

      public void expectError(Throwable expected, long timeoutMillis) throws InterruptedException {
        error.expectCompletion(timeoutMillis, String.format("Subscriber %s did not call `registerOnError(%s)`", sub(), expected));
        if (error.value() != expected) {
          env.flop(String.format("Subscriber %s called `registerOnError(%s)` rather than `registerOnError(%s)`", sub(), error.value(), expected));
        }
      }

      public void expectNone() throws InterruptedException {
        expectNone(env.defaultTimeoutMillis());
      }

      public void expectNone(long withinMillis) throws InterruptedException {
        elements.expectNone(withinMillis, "Expected nothing");
      }

      public void verifyNoAsyncErrors() {
        env.verifyNoAsyncErrors();
      }
    }
  }

  public interface SubscriberProbe<T> {
    /**
     * Must be called by the test subscriber when it has received the `onSubscribe` event.
     */
    void registerOnSubscribe(SubscriberPuppet puppet);

    /**
     * Must be called by the test subscriber when it has received an`onNext` event.
     */
    void registerOnNext(T element);

    /**
     * Must be called by the test subscriber when it has received an `onComplete` event.
     */
    void registerOnComplete();

    /**
     * Must be called by the test subscriber when it has received an `onError` event.
     */
    void registerOnError(Throwable cause);

  }

  public interface SubscriberPuppet {
    void triggerShutdown();

    void triggerRequest(long elements);

    void signalCancel();
  }

  public void notVerified() {
    throw new SkipException("Not verified using this TCK.");
  }
}