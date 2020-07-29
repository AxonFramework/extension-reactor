package org.axonframework.extensions.reactor.commandhandling.callbacks;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.commandhandling.gateway.RetryScheduler;
import org.axonframework.extensions.reactor.messaging.ReactorMessageDispatchInterceptor;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.junit.jupiter.api.*;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.axonframework.commandhandling.GenericCommandResultMessage.asCommandResultMessage;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link ReactorCallback}.
 *
 * @author Stefan Dragisic
 */
class ReactorCallbackTest {

    private static final CommandMessage<Object> COMMAND_MESSAGE = GenericCommandMessage.asCommandMessage("Test");
    private static final CommandResultMessage<String> COMMAND_RESPONSE_MESSAGE =
            asCommandResultMessage("Hello reactive world");
    private volatile ReactorCallback<Object, Object> testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new ReactorCallback<>();
    }

    @Test
    void testOnSuccessCallback() throws Exception {
        testSubject.onResult(COMMAND_MESSAGE, COMMAND_RESPONSE_MESSAGE);

        StepVerifier.create(testSubject)
                    .expectSubscription()
                    .expectNext(COMMAND_RESPONSE_MESSAGE)
                    .expectComplete()
                    .verify();
    }

    @Test
    void testOnErrorCallback() {
        RuntimeException exception = new RuntimeException();
        testSubject.onResult(COMMAND_MESSAGE, asCommandResultMessage(exception));

        StepVerifier.create(testSubject)
                    .expectSubscription()
                    .expectError(RuntimeException.class)
                    .verify();
    }

    @Test
    void testOnSuccessForLimitedTime_Timeout() throws Exception {
        testSubject.onResult(COMMAND_MESSAGE, COMMAND_RESPONSE_MESSAGE);
        final CountDownLatch successCountDownLatch = new CountDownLatch(1);

        testSubject
                .delaySubscription(Duration.ofSeconds(2))
                .subscribe(it -> successCountDownLatch.countDown());

        assertTrue(successCountDownLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    void testOnErrorForLimitedTime_Timeout() throws Exception {
        testSubject.onResult(COMMAND_MESSAGE, COMMAND_RESPONSE_MESSAGE);
        final CountDownLatch successCountDownLatch = new CountDownLatch(1);

        testSubject
                .delaySubscription(Duration.ofSeconds(2))
                .subscribe(it -> successCountDownLatch.countDown());

        assertFalse(successCountDownLatch.await(1, TimeUnit.SECONDS));
    }

    @Test
    void testOnResultReturnsMessageWithTimeoutExceptionOnTimeout() {
        StepVerifier.create(testSubject.timeout(Duration.ofSeconds(1)))
                    .expectSubscription()
                    .expectError(TimeoutException.class)
                    .verify();
    }

    @Test
    void testReactorDispatchInterceptorWithDefaultCommandGateway() {
        MessageDispatchInterceptor<CommandMessage<?>> mockCommandMessageTransformer = (ReactorMessageDispatchInterceptor<CommandMessage<?>>) message -> message
                .map(commandMessage -> new GenericCommandMessage<>("intercepted" + commandMessage.getPayload()));
        CommandBus mockCommandBus = mock(CommandBus.class);
        RetryScheduler mockRetryScheduler = mock(RetryScheduler.class);
        DefaultCommandGateway commandGateway = DefaultCommandGateway.builder()
                                                                    .commandBus(mockCommandBus)
                                                                    .retryScheduler(mockRetryScheduler)
                                                                    .dispatchInterceptors(mockCommandMessageTransformer)
                                                                    .build();

        doAnswer(invocation -> {
            CommandMessage commandMessage = (CommandMessage) invocation.getArguments()[0];
            ((CommandCallback) invocation.getArguments()[1])
                    .onResult(commandMessage, asCommandResultMessage(commandMessage.getPayload()));
            return null;
        }).when(mockCommandBus).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        String result = (String) commandGateway.send("Command").join();

        assertEquals("interceptedCommand", result);
    }
}
