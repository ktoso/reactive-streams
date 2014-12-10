package org.reactivestreams.tck;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.Annotations.Additional;
import org.reactivestreams.tck.support.TCKVerificationSupport;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
* Validates that the TCK's {@link org.reactivestreams.tck.PublisherVerification} fails with nice human readable errors.
* <b>Important: Please note that all Publishers implemented in this file are *wrong*!</b>
*/
public class PublisherVerificationTest extends TCKVerificationSupport {

  final int DEFAULT_TIMEOUT_MILLIS = 100;
  final int GC_TIMEOUT_MILLIS = 300;


  @Test
  public void spec101_subscriptionRequestMustResultInTheCorrectNumberOfProducedElements_shouldFailBy_ExpectingOnError() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        noopPublisherVerification().spec101_subscriptionRequestMustResultInTheCorrectNumberOfProducedElements();
      }
    }, "produced no element after first");
  }

  @Test
  public void spec102_maySignalLessThanRequestedAndTerminateSubscription_shouldFailBy_notReceivingAnyElement() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        noopPublisherVerification().spec102_maySignalLessThanRequestedAndTerminateSubscription();
      }
    }, "Did not receive expected element");
  }

  @Test
  public void spec102_maySignalLessThanRequestedAndTerminateSubscription_shouldFailBy_receivingTooManyElements() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        demandIgnoringSynchronousPublisherVerification().spec102_maySignalLessThanRequestedAndTerminateSubscription();
      }
    }, "Expected end-of-stream but got 3");
  }

  @Test
  public void spec103_mustSignalOnMethodsSequentially_shouldFailBy_concurrentlyAccessingOnNext() throws Throwable {
    final AtomicInteger startedSignallingThreads = new AtomicInteger(0);
    // this is an arbitrary number, we jsut need "many threads" to try to force an concurrent access scenario
    final int maxSignallingThreads = 10;

    final ExecutorService signallersPool = Executors.newFixedThreadPool(maxSignallingThreads);
    final AtomicBoolean concurrentAccessCaused = new AtomicBoolean(false);


    // highly specialised threadpool driven publisher which aims to FORCE concurrent access,
    // so that we can confirm the test is able to catch this.
    final Publisher<Integer> concurrentAccessPublisher = new Publisher<Integer>() {
      @Override public void subscribe(final Subscriber<? super Integer> s) {
        s.onSubscribe(new Subscription() {
          @Override public void request(final long n) {
            Runnable signalling = new Runnable() {

              @Override public void run() {
                for (long i = 0; i < n; i++) {
                  try {
                    // shutdown cleanly in when the threadpool is shutting down
                    if (Thread.interrupted()) {
                      return;
                    }

                    s.onNext((int) i);
                  } catch (Exception ex) {
                    // signal others to shut down
                    signallersPool.shutdownNow();

                    if (ex.getMessage().contains("Expected latch to be open")) {
                      if (!concurrentAccessCaused.getAndSet(true)) {
                        throw new RuntimeException("Concurrent access detected", ex);
                      } else {
                        // error signalled once already, stop more errors from propagating
                        return;
                      }
                    } else {
                      throw new RuntimeException(ex);
                    }
                  }
                }
              }
            };

            // must be guarded like this in case a Subscriber triggers request() synchronously from it's onNext()
            while (startedSignallingThreads.getAndAdd(1) < maxSignallingThreads && !signallersPool.isShutdown()) {
              try {
                signallersPool.submit(signalling);
              } catch (RejectedExecutionException ex) {
                // ignore, should be safe as it means the pool is shutting down -> which means we triggered the problem we wanted to
                return;
              }
            }
          }

          @Override public void cancel() {
            // noop
          }
        });
      }
    };

    try {
      requireTestFailure(new ThrowingRunnable() {
        @Override public void run() throws Throwable {
          customPublisherVerification(concurrentAccessPublisher).spec103_mustSignalOnMethodsSequentially();
        }
      }, "Expected latch to be open during onNext call"); // TODO error message could be better, to show that the concurrent signalling is the problem
    } finally {
      signallersPool.shutdownNow();
      signallersPool.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void spec104_mustSignalOnErrorWhenFails_shouldFail() throws Throwable {
    final Publisher<Integer> invalidErrorPublisher = new Publisher<Integer>() {
      @Override public void subscribe(Subscriber<? super Integer> s) {
        throw new RuntimeException("It is not valid to throw here!");
      }
    };
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(SKIP, invalidErrorPublisher).spec104_mustSignalOnErrorWhenFails();
      }
    }, "Publisher threw exception (java.lang.RuntimeException: It is not valid to throw here!) instead of signalling error via onError!");
  }

  @Test
  public void spec105_mustSignalOnCompleteWhenFiniteStreamTerminates_shouldFail() throws Throwable {
    final Publisher<Integer> forgotToSignalCompletionPublisher = new Publisher<Integer>() {
      @Override public void subscribe(final Subscriber<? super Integer> s) {
        s.onSubscribe(new Subscription() {
          int signal = 0;

          @Override public void request(long n) {
            for (int i = 0; i < n; i++) {
              s.onNext(signal);
              signal += 1;
            }
            // forget to signal completion!
          }

          @Override public void cancel() {
            // noop
          }
        });
      }
    };

    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(forgotToSignalCompletionPublisher).spec105_mustSignalOnCompleteWhenFiniteStreamTerminates();
      }
    }, "Expected end-of-stream but got 3");
  }

  @Test
  public void spec107_mustNotEmitFurtherSignalsOnceOnCompleteHasBeenSignalled_shouldFailForNotCompletingPublisher() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        demandIgnoringSynchronousPublisherVerification().spec107_mustNotEmitFurtherSignalsOnceOnCompleteHasBeenSignalled();
      }
    }, "Expected end-of-stream but got");
  }

  @Test
  public void spec107_mustNotEmitFurtherSignalsOnceOnCompleteHasBeenSignalled_shouldFailForPublisherWhichCompletesButKeepsServingData() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {

              boolean completed = false;

              @Override public void request(long n) {
                // emit one element
                s.onNext(0);

                // and "complete"
                // but keep signalling data if more demand comes in anyway!
                if (!completed) {
                  s.onComplete();
                }

              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        }).spec107_mustNotEmitFurtherSignalsOnceOnCompleteHasBeenSignalled();
      }
    }, "Unexpected element 0 received after stream completed");
  }

  @Additional @Test
  public void spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice_shouldFailBy_skippingSinceOptional() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        noopPublisherVerification().spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice();
      }
    }, "Skipped because tested publisher does NOT implement this OPTIONAL requirement.");
  }

  @Additional @Test
  public void spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice_shouldFailBy_signallingWrongException() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          volatile Subscriber subscriber = null;

          @Override public void subscribe(Subscriber<? super Integer> s) {
            if (subscriber == null) {
              this.subscriber = s;
              s.onSubscribe(new Subscription() {
                @Override public void request(long n) {
                  // noop
                }

                @Override public void cancel() {
                   // noop
                }
              });
            } else {
              // on error properly, but forget to include rule number
              s.onError(new RuntimeException("This is the wrong exception type, but in the right place [1.10]"));
            }
          }
        }).spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice();
      }
    }, "Got java.lang.RuntimeException but expected java.lang.IllegalStateException");
  }

  @Additional @Test
  public void spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice_shouldFailBy_missingSpecReferenceInRightException() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          volatile Subscriber subscriber = null;

          @Override public void subscribe(Subscriber<? super Integer> s) {
            if (subscriber == null) {
              this.subscriber = s;
              s.onSubscribe(new Subscription() {
                @Override public void request(long n) {
                  // noop
                }

                @Override public void cancel() {
                   // noop
                }
              });
            } else {
              // on error properly, but forget to include rule number
              s.onError(new IllegalStateException("Sorry, I can only support one subscriber, and I have one already. See rule [XXX]"));
            }
          }
        }).spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice();
      }
    }, "Got expected exception [class java.lang.IllegalStateException] but missing message part [1.10]");
  }

  @Test
  public void spec111_maySupportMultiSubscribe_shouldFailBy_actuallyPass() throws Throwable {
    noopPublisherVerification().spec111_maySupportMultiSubscribe();
  }

  @Additional @Test
  public void spec112_mayRejectCallsToSubscribeIfPublisherIsUnableOrUnwillingToServeThemRejectionMustTriggerOnErrorInsteadOfOnSubscribe_shouldFail() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(SKIP, new Publisher<Integer>() {
          @Override public void subscribe(Subscriber<? super Integer> s) {
          }
        }).spec112_mayRejectCallsToSubscribeIfPublisherIsUnableOrUnwillingToServeThemRejectionMustTriggerOnErrorInsteadOfOnSubscribe();
      }
    }, "Should have received onError");
  }

  @Additional @Test
  public void spec112_mayRejectCallsToSubscribeIfPublisherIsUnableOrUnwillingToServeThemRejectionMustTriggerOnErrorInsteadOfOnSubscribe_actuallyPass() throws Throwable {
    customPublisherVerification(SKIP, new Publisher<Integer>() {
      @Override public void subscribe(Subscriber<? super Integer> s) {
        s.onError(new RuntimeException("Sorry, I'm busy now. Call me later."));
      }
    }).spec112_mayRejectCallsToSubscribeIfPublisherIsUnableOrUnwillingToServeThemRejectionMustTriggerOnErrorInsteadOfOnSubscribe();
  }

  @Additional @Test
  public void spec113_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingManyUpfront_shouldFailBy_expectingOnError() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            final Random rnd = new Random();
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                for (int i = 0; i < n; i++) {
                  s.onNext(rnd.nextInt());
                }
              }

              @Override public void cancel() {

              }
            });
          }
        }).spec113_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingManyUpfront();
      }
    }, "Expected elements to be signaled in the same sequence to 1st and 2nd subscribers: Lists differ at element " /* ... */);
  }


  @Test
  public void spec302_mustAllowSynchronousRequestCallsFromOnNextAndOnSubscribe_shouldFailBy_reportingAsyncError() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        onErroringPublisherVerification().spec302_mustAllowSynchronousRequestCallsFromOnNextAndOnSubscribe();
      }
    }, "Async error during test execution: ");
  }

  @Test
  public void spec303_mustNotAllowUnboundedRecursion_shouldFailBy_informingAboutTooDeepStack() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                s.onNext(0); // naive reccursive call, would explode with StackOverflowException
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        }).spec303_mustNotAllowUnboundedRecursion();
      }
    }, /* Got 2 onNext calls within thread: ... */ "yet expected recursive bound was 1");
  }

  @Test
  public void spec306_afterSubscriptionIsCancelledRequestMustBeNops_shouldFailBy_unexpectedElement() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        demandIgnoringSynchronousPublisherVerification().spec306_afterSubscriptionIsCancelledRequestMustBeNops();
      }
    }, "Did not expect an element but got 0");
  }

  @Test
  public void spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops_shouldPass() throws Throwable {
    demandIgnoringSynchronousPublisherVerification().spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops();
  }

  @Test
  public void spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops_shouldFailBy_unexpectedErrorInCancelling() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                // noop
              }

              @Override public void cancel() {
                s.onError(new RuntimeException("Invalid, don't do this!")); // illegal error signalling!
              }
            });
          }
        }).spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops();
      }
    }, "Async error during test execution: Invalid, don't do this!");
  }

  @Test
  public void spec309_requestZeroMustSignalIllegalArgumentException_shouldFailBy_expectingOnError() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        noopPublisherVerification().spec309_requestZeroMustSignalIllegalArgumentException();
      }
    }, "Expected onError");
  }

  @Test
  public void spec309_requestNegativeNumberMustSignalIllegalArgumentException_shouldFailBy_expectingOnError() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        noopPublisherVerification().spec309_requestNegativeNumberMustSignalIllegalArgumentException();
      }
    }, "Expected onError");
  }

  @Test
  public void spec312_cancelMustMakeThePublisherToEventuallyStopSignaling_shouldFailBy_havingEmitedMoreThanRequested() throws Throwable {
    final ExecutorService pool = Executors.newFixedThreadPool(2);

    try {
      requireTestFailure(new ThrowingRunnable() {
        @Override public void run() throws Throwable {
          demandIgnoringAsynchronousPublisherVerification(pool).spec312_cancelMustMakeThePublisherToEventuallyStopSignaling();
        }
      }, /*Publisher signalled [...] */ ", which is more than the signalled demand: ");
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber_shouldFailBy_keepingTheReferenceLongerThanNeeded() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          Subscriber subs = null;

          @Override public void subscribe(final Subscriber<? super Integer> s) {
            subs = s; // keep the reference

            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                for (int i = 0; i < n; i++) {
                  s.onNext((int) n);
                }
              }

              @Override public void cancel() {
                // noop, we still keep the reference!
              }
            });
          }
        }).spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber();
      }
    }, "did not drop reference to test subscriber after subscription cancellation");
  }

  @Test
  public void spec317_mustSupportAPendingElementCountUpToLongMaxValue_shouldFail_onAsynchDemandIgnoringPublisher() throws Throwable {
    final ExecutorService pool = Executors.newFixedThreadPool(10); // TODO fixme, why 10 - no good reason

    try {
      requireTestFailure(new ThrowingRunnable() {
        @Override public void run() throws Throwable {
          demandIgnoringAsynchronousPublisherVerification(pool).spec317_mustSupportAPendingElementCountUpToLongMaxValue();
        }
      }, "Expected end-of-stream but got");
    } finally {
      pool.shutdownNow();
      pool.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void spec317_mustSupportAPendingElementCountUpToLongMaxValue_shouldFail_onSynchDemandIgnoringPublisher() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        demandIgnoringSynchronousPublisherVerification().spec317_mustSupportAPendingElementCountUpToLongMaxValue();
      }
    }, "Received more than bufferSize (32) onNext signals. The Publisher probably emited more signals than expected!");
  }

  @Test
  public void spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue_shouldFail_onAsynchDemandIgnoringPublisher() throws Throwable {
    final ExecutorService signallersPool = Executors.newFixedThreadPool(2);
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        demandIgnoringAsynchronousPublisherVerification(signallersPool).spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue();
      }
    }, "Expected onError(java.lang.IllegalStateException)");
  }

  @Test
  public void spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue_shouldFail_onSynchDemandIgnoringPublisher() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          long demand = 0;

          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                // it does not protect from demand overflow!
                demand += n;
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        }).spec317_mustSignalOnErrorWhenPendingAboveLongMaxValue();
      }
    }, "Expected onError(java.lang.IllegalStateException)");
  }

  @Test
  public void spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue_shouldFail_overflowingDemand() throws Throwable {
    requireTestFailure(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        customPublisherVerification(new Publisher<Integer>() {
          long demand = 0;

          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                demand += n;

                // this is a mistake, it should still be able to accumulate such demand
                if (demand == Long.MAX_VALUE)
                  s.onError(new IllegalStateException("I'm signalling onError too soon! Cumulative demand equal to Long.MAX_VALUE is OK by the spec."));

                s.onNext(0);
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        }).spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue();
      }
    }, "Async error during test execution: I'm signalling onError too soon!");
  }


  // FAILING IMPLEMENTATIONS //

  final Publisher<Integer> SKIP = null;

  /**
   * Verification using a Publisher that never publishes any element
   */
  final PublisherVerification<Integer> noopPublisherVerification() {
    return new PublisherVerification<Integer>(newTestEnvironment(), GC_TIMEOUT_MILLIS) {
      @Override public Publisher<Integer> createPublisher(long elements) {

        return new Publisher<Integer>() {
          @Override public void subscribe(Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                // noop
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        };

      }

      @Override public Publisher<Integer> createErrorStatePublisher() {
        return SKIP;
      }
    };
  }

  /**
   * Verification using a Publisher that never publishes any element
   */
  final PublisherVerification<Integer> onErroringPublisherVerification() {
    return new PublisherVerification<Integer>(newTestEnvironment(), GC_TIMEOUT_MILLIS) {
      @Override public Publisher<Integer> createPublisher(long elements) {

        return new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {
                s.onError(new RuntimeException("Erroring for no good reason!"));
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        };

      }

      @Override public Publisher<Integer> createErrorStatePublisher() {
        return SKIP;
      }
    };
  }

  /**
   * Custom Verification using given Publishers
   */
  final PublisherVerification<Integer> customPublisherVerification(final Publisher<Integer> pub) {
    return customPublisherVerification(pub, SKIP);
  }

  /**
   * Custom Verification using given Publishers
   */
  final PublisherVerification<Integer> customPublisherVerification(final Publisher<Integer> pub, final Publisher<Integer> errorPub) {
    return new PublisherVerification<Integer>(newTestEnvironment(), GC_TIMEOUT_MILLIS) {
      @Override public Publisher<Integer> createPublisher(long elements) {
        return pub;
      }

      @Override public Publisher<Integer> createErrorStatePublisher() {
        return errorPub;
      }
    };
  }

  /**
   * Verification using a Publisher that publishes elements even with no demand available
   */
  final PublisherVerification<Integer> demandIgnoringSynchronousPublisherVerification() {
    return new PublisherVerification<Integer>(newTestEnvironment(), GC_TIMEOUT_MILLIS) {
      @Override public Publisher<Integer> createPublisher(long elements) {

        return new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(long n) {

                for (long i = 0; i <= n; i++) {
                  // one too much
                  s.onNext((int) i);
                }
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        };

      }

      @Override public Publisher<Integer> createErrorStatePublisher() {
        return SKIP;
      }
    };
  }

  /**
   * Verification using a Publisher that publishes elements even with no demand available, from multiple threads (!).
   *
   * Please note that exceptions thrown from onNext *will be swallowed* – reason being this verification is used to check
   * very specific things about error reporting - from the "TCK Tests", we do not have any assertions on thrown exceptions.
   */
  final PublisherVerification<Integer> demandIgnoringAsynchronousPublisherVerification(final ExecutorService signallersPool) {
    return demandIgnoringAsynchronousPublisherVerification(signallersPool, true);
  }

  /**
   * Verification using a Publisher that publishes elements even with no demand available, from multiple threads (!).
   */
  final PublisherVerification<Integer> demandIgnoringAsynchronousPublisherVerification(final ExecutorService signallersPool, final boolean swallowOnNextExceptions) {
    final AtomicInteger startedSignallingThreads = new AtomicInteger(0);
    final int maxSignallingThreads = 2;

    final AtomicBoolean concurrentAccessCaused = new AtomicBoolean(false);

    return new PublisherVerification<Integer>(newTestEnvironment(), GC_TIMEOUT_MILLIS) {
      @Override public Publisher<Integer> createPublisher(long elements) {

        return new Publisher<Integer>() {
          @Override public void subscribe(final Subscriber<? super Integer> s) {
            s.onSubscribe(new Subscription() {
              @Override public void request(final long n) {
                Runnable signalling = new Runnable() {

                  @Override public void run() {
                    for (long i = 0; i <= n; i++) {
                      // one signal too much

                      try {
                        final long signal = i;
                        signallersPool.execute(new Runnable() {
                          @Override public void run() {
                            try {
                              s.onNext((int) signal);
                            } catch (Exception ex) {
                              if (!swallowOnNextExceptions) {
                                throw new RuntimeException("onNext threw an exception!", ex);
                              } else {
                                // yes, swallow the exception, we're not asserting and they'd just end up being logged (stdout),
                                // which we do not need in this specific PublisherVerificationTest
                              }
                            }
                          }
                        });
                      } catch (Exception ex) {
                        if (ex.getMessage().contains("Expected latch to be open")) {
                          if (concurrentAccessCaused.compareAndSet(false, true)) {
                            throw new RuntimeException("Concurrent access detected", ex);
                          } else {
                            // error signalled once already, stop more errors from propagating
                            return;
                          }
                        } else {
                          if (concurrentAccessCaused.get()) {
                            return;
                          } else {
                            throw new RuntimeException(ex);
                          }
                        }
                      }
                    }
                  }
                };

                // must be guarded like this in case a Subscriber triggers request() synchronously from it's onNext()
                while (startedSignallingThreads.getAndAdd(1) < maxSignallingThreads) {
                  signallersPool.submit(signalling);
                }
              }

              @Override public void cancel() {
                // noop
              }
            });
          }
        };

      }

      @Override public Publisher<Integer> createErrorStatePublisher() {
        return SKIP;
      }
    };
  }

  private TestEnvironment newTestEnvironment() {
    return new TestEnvironment(DEFAULT_TIMEOUT_MILLIS);
  }


}
