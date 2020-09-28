/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package tests.integration.com.microsoft.azure.sdk.iot.iothub;


import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.DeviceMethodData;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.Pair;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.Property;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.TwinPropertyCallBack;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import com.microsoft.azure.sdk.iot.service.*;
import com.microsoft.azure.sdk.iot.service.devicetwin.DeviceMethod;
import com.microsoft.azure.sdk.iot.service.devicetwin.DeviceTwin;
import com.microsoft.azure.sdk.iot.service.devicetwin.DeviceTwinClientOptions;
import com.microsoft.azure.sdk.iot.service.devicetwin.DeviceTwinDevice;
import com.microsoft.azure.sdk.iot.service.exceptions.IotHubException;
import org.junit.*;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import tests.integration.com.microsoft.azure.sdk.iot.helpers.Tools;
import tests.integration.com.microsoft.azure.sdk.iot.helpers.*;
import tests.integration.com.microsoft.azure.sdk.iot.helpers.annotations.ContinuousIntegrationTest;
import tests.integration.com.microsoft.azure.sdk.iot.helpers.annotations.IotHubTest;
import tests.integration.com.microsoft.azure.sdk.iot.helpers.annotations.StandardTierHubOnlyTest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.util.*;

import static junit.framework.TestCase.*;

/**
 * Test class containing all tests to be run on JVM and android pertaining to multiplexing with the MultiplexingClient class
 */
@IotHubTest
@RunWith(Parameterized.class)
public class MultiplexingClientTests extends IntegrationTest
{
    private static final int MAX_DEVICE_MULTIPLEX_COUNT = 1000;
    private static final int MAX_DEVICE_MULTIPLEX_COUNT_AMQPS_WS = 500;
    private static final int DEVICE_MULTIPLEX_COUNT = 10;

    private static final int MESSAGE_SEND_TIMEOUT_MILLIS = 60 * 1000;
    private static final int FAULT_INJECTION_RECOVERY_TIMEOUT_MILLIS = 2 * 60 * 1000;
    private static final int FAULT_INJECTION_TIMEOUT_MILLIS = 60 * 1000;
    private static final int DEVICE_METHOD_SUBSCRIBE_TIMEOUT_MILLISECONDS = 60 * 1000;
    private static final int TWIN_SUBSCRIBE_TIMEOUT_MILLIS = 60 * 1000;
    private static final long MAXIMUM_TIME_TO_WAIT_FOR_DESIRED_PROPERTY_SUBSCRIPTION_ACKNOWLEDGEMENT = 500; // .5 seconds
    private static final long MAXIMUM_TIME_TO_WAIT_FOR_REPORTED_PROPERTY_ACKNOWLEDGEMENT = 1000; // 1 second
    private static final int DESIRED_PROPERTY_CALLBACK_TIMEOUT_MILLIS = 60 * 1000;
    private static final int DEVICE_SESSION_OPEN_TIMEOUT = 60 * 1000;
    private static final int DEVICE_SESSION_CLOSE_TIMEOUT = 60 * 1000;

    protected static String iotHubConnectionString = "";

    private static ServiceClient serviceClient;
    private static RegistryManager registryManager;

    protected static HttpProxyServer proxyServer;
    protected static String testProxyHostname = "127.0.0.1";
    protected static int testProxyPort = 8849;
    protected static final String testProxyUser = "proxyUsername";
    protected static final char[] testProxyPass = "1234".toCharArray();

    private static final int MULTIPLEXING_CLIENT_TEST_TIMEOUT_MILLISECONDS = 10 * 60 * 1000;

    @Parameterized.Parameters(name = "{0}")
    public static Collection inputs() throws Exception
    {
        iotHubConnectionString = Tools.retrieveEnvironmentVariableValue(TestConstants.IOT_HUB_CONNECTION_STRING_ENV_VAR_NAME);
        isBasicTierHub = Boolean.parseBoolean(Tools.retrieveEnvironmentVariableValue(TestConstants.IS_BASIC_TIER_HUB_ENV_VAR_NAME));
        isPullRequest = Boolean.parseBoolean(Tools.retrieveEnvironmentVariableValue(TestConstants.IS_PULL_REQUEST));

        registryManager = RegistryManager.createFromConnectionString(iotHubConnectionString, RegistryManagerOptions.builder().httpReadTimeout(HTTP_READ_TIMEOUT).build());
        serviceClient = ServiceClient.createFromConnectionString(iotHubConnectionString, IotHubServiceClientProtocol.AMQPS);
        serviceClient.open();

        return Arrays.asList(
                new Object[][]
                        {
                                {IotHubClientProtocol.AMQPS},
                                {IotHubClientProtocol.AMQPS_WS}
                        });
    }

    public MultiplexingClientTests(IotHubClientProtocol protocol)
    {
        this.testInstance = new MultiplexingClientTestInstance(protocol);

        // Some CI tests in this suite take upwards of 6 minutes to run normally, so this timeout overrides the default timeout of 5 minutes
        timeout = new Timeout(MULTIPLEXING_CLIENT_TEST_TIMEOUT_MILLISECONDS);
    }

    public MultiplexingClientTestInstance testInstance;

    public class MultiplexingClientTestInstance
    {
        public IotHubClientProtocol protocol;
        public Device[] deviceIdentityArray;
        public DeviceClient[] deviceClientArray;
        public MultiplexingClient multiplexingClient;

        public MultiplexingClientTestInstance(IotHubClientProtocol protocol)
        {
            this.protocol = protocol;
        }

        public void setup(int multiplexingDeviceSessionCount) throws InterruptedException, IotHubException, IOException, URISyntaxException
        {
            setup(multiplexingDeviceSessionCount, null);
        }

        public void setup(int multiplexingDeviceSessionCount, ProxySettings proxySettings) throws InterruptedException, IotHubException, IOException, URISyntaxException
        {
            deviceIdentityArray = new Device[multiplexingDeviceSessionCount];
            deviceClientArray = new DeviceClient[multiplexingDeviceSessionCount];
            String uuid = UUID.randomUUID().toString();

            for (int i = 0; i < multiplexingDeviceSessionCount; i++)
            {
                String deviceId = "java-device-client-e2e-test-multiplexing".concat(i + "-" + uuid);

                deviceIdentityArray[i] = Device.createFromId(deviceId, null, null);
                Tools.addDeviceWithRetry(registryManager, deviceIdentityArray[i]);
            }

            Thread.sleep(2000);

            this.multiplexingClient = new MultiplexingClient(this.protocol, proxySettings);
            for (int i = 0; i < multiplexingDeviceSessionCount; i++)
            {
                this.deviceClientArray[i] = new DeviceClient(registryManager.getDeviceConnectionString(deviceIdentityArray[i]), this.protocol);
                this.multiplexingClient.registerDeviceClient(this.deviceClientArray[i]);
            }
        }

        public void dispose()
        {
            try
            {
                for (Device deviceIdentity : deviceIdentityArray)
                {
                    registryManager.removeDevice(deviceIdentity);
                }
            }
            catch (Exception e)
            {
                //ignore the exception, don't care if tear down wasn't successful for this test
            }
        }
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if (registryManager != null)
        {
            registryManager.close();
        }

        if (serviceClient != null)
        {
            serviceClient.close();
        }
    }

    @After
    public void tearDownTest()
    {
        testInstance.dispose();
    }

    @BeforeClass
    public static void startProxy()
    {
        proxyServer = DefaultHttpProxyServer.bootstrap()
                .withPort(testProxyPort)
                .withProxyAuthenticator(new BasicProxyAuthenticator(testProxyUser, testProxyPass))
                .start();
    }

    @AfterClass
    public static void stopProxy()
    {
        proxyServer.stop();
    }

    @Test
    public void sendMessages() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);
        testInstance.multiplexingClient.open();

        testSendingMessagesFromMultiplexedClients(testInstance.deviceClientArray);

        testInstance.multiplexingClient.close();
    }

    @ContinuousIntegrationTest
    @Test
    public void canReopenClosedMultiplexingClient() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);

        // Open and close the connection once
        testInstance.multiplexingClient.open();
        testInstance.multiplexingClient.close();

        // Re-open the connection and verify that it can still send telemetry
        testInstance.multiplexingClient.open();
        testSendingMessagesFromMultiplexedClients(testInstance.deviceClientArray);
        testInstance.multiplexingClient.close();
    }

    @ContinuousIntegrationTest
    @Test
    public void sendMessagesMaxDevicesAllowed() throws Exception
    {
        // Right now, AMQP connections can do up to 1000 devices which is consistent with the IoTHub advertised limit
        // But AMQPS_WS is limited to ~500 for some reason. Still needs investigation.
        if (testInstance.protocol == IotHubClientProtocol.AMQPS)
        {
            testInstance.setup(MAX_DEVICE_MULTIPLEX_COUNT);
        }
        else
        {
            testInstance.setup(MAX_DEVICE_MULTIPLEX_COUNT_AMQPS_WS);
        }

        testInstance.multiplexingClient.open();

        testSendingMessagesFromMultiplexedClients(testInstance.deviceClientArray);

        testInstance.multiplexingClient.close();
    }

    @Test
    public void sendMessagesWithProxy() throws Exception
    {
        if (testInstance.protocol != IotHubClientProtocol.AMQPS_WS)
        {
            // only AMQPS_WS supports proxies
            return;
        }

        Proxy testProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(testProxyHostname, testProxyPort));
        ProxySettings proxySettings = new ProxySettings(testProxy, testProxyUser, testProxyPass);

        //re-setup test instance to use proxy instead
        testInstance.setup(DEVICE_MULTIPLEX_COUNT, proxySettings);
        testInstance.multiplexingClient.open();

        testSendingMessagesFromMultiplexedClients(testInstance.deviceClientArray);

        testInstance.multiplexingClient.close();
    }

    private static void testSendingMessagesFromMultiplexedClients(DeviceClient[] multiplexedClients) throws InterruptedException
    {
        for (DeviceClient client : multiplexedClients)
        {
            testSendingMessageFromMultiplexedClient(client);
        }
    }

    private static void testSendingMessageFromMultiplexedClient(DeviceClient multiplexedClient, Message message, String timeoutErrorMessage) throws InterruptedException {
        Success messageSendSuccess = new Success();
        EventCallback messageSentCallback = new EventCallback(IotHubStatusCode.OK_EMPTY);
        multiplexedClient.sendEventAsync(message, messageSentCallback, messageSendSuccess);

        long startTime = System.currentTimeMillis();
        while (!messageSendSuccess.wasCallbackFired())
        {
            Thread.sleep(200);

            if (System.currentTimeMillis() - startTime > MESSAGE_SEND_TIMEOUT_MILLIS)
            {
                fail(timeoutErrorMessage);
            }
        }

        assertTrue("Unexpected callback result: " + messageSendSuccess.getCallbackStatusCode(), messageSendSuccess.getResult());
    }

    private static void testSendingMessageFromMultiplexedClient(DeviceClient multiplexedClient) throws InterruptedException {
        testSendingMessageFromMultiplexedClient(multiplexedClient, new Message("some payload"), "Timed out waiting for sent message to be acknowledged");
    }

    @Test
    @StandardTierHubOnlyTest
    public void receiveMessagesIncludingProperties() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);
        testInstance.multiplexingClient.open();

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            String expectedMessageCorrelationId = UUID.randomUUID().toString();
            MessageCallback messageCallback = new MessageCallback(expectedMessageCorrelationId);
            testInstance.deviceClientArray[i].setMessageCallback(messageCallback, null);

            com.microsoft.azure.sdk.iot.service.Message serviceMessage = new com.microsoft.azure.sdk.iot.service.Message("some payload");
            serviceMessage.setCorrelationId(expectedMessageCorrelationId);
            serviceClient.send(testInstance.deviceIdentityArray[i].getDeviceId(), serviceMessage);

            long startTime = System.currentTimeMillis();
            while (!messageCallback.messageCallbackFired)
            {
                Thread.sleep(200);

                if (System.currentTimeMillis() - startTime > MESSAGE_SEND_TIMEOUT_MILLIS)
                {
                    fail("Timed out waiting for message to be received");
                }
            }

            assertTrue("Message callback fired, but unexpected message was received", messageCallback.expectedMessageReceived);
        }

        testInstance.multiplexingClient.close();
    }

    private class MessageCallback implements com.microsoft.azure.sdk.iot.device.MessageCallback
    {
        public boolean messageCallbackFired = false;
        public boolean expectedMessageReceived = false;

        final String expectedCorrelationId;

        public MessageCallback(String expectedMessageCorrelationId)
        {
            this.expectedCorrelationId = expectedMessageCorrelationId;
        }

        @Override
        public IotHubMessageResult execute(Message message, Object callbackContext)
        {
            messageCallbackFired = true;
            if (message.getCorrelationId().equals(expectedCorrelationId))
            {
                expectedMessageReceived = true;
            }

            return IotHubMessageResult.COMPLETE;
        }
    }

    @Test
    @StandardTierHubOnlyTest
    public void invokeMethodSucceed() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);
        testInstance.multiplexingClient.open();
        DeviceMethod deviceMethodServiceClient = DeviceMethod.createFromConnectionString(iotHubConnectionString);

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            // Subscribe to methods on the multiplexed client
            String expectedMethodName = UUID.randomUUID().toString();
            DeviceMethodCallback deviceMethodCallback = new DeviceMethodCallback(expectedMethodName);
            Success methodsSubscribedSuccess = new Success();
            testInstance.deviceClientArray[i].subscribeToDeviceMethod(deviceMethodCallback, null, new IotHubEventCallback() {
                @Override
                public void execute(IotHubStatusCode responseStatus, Object callbackContext) {
                    ((Success) callbackContext).setCallbackStatusCode(responseStatus);
                    ((Success) callbackContext).setResult(responseStatus == IotHubStatusCode.OK_EMPTY);
                    ((Success) callbackContext).callbackWasFired();
                }
            }, methodsSubscribedSuccess);

            // Wait for methods subscription to be acknowledged by hub
            long startTime = System.currentTimeMillis();
            while (methodsSubscribedSuccess.wasCallbackFired())
            {
                Thread.sleep(200);

                if (System.currentTimeMillis() - startTime > DEVICE_METHOD_SUBSCRIBE_TIMEOUT_MILLISECONDS)
                {
                    throw new AssertionError("Timed out waiting for device method subscription to be acknowledged");
                }
            }

            // Give the method subscription some extra buffer time before invoking the method
            Thread.sleep(1000);

            // Invoke method on the multiplexed device
            deviceMethodServiceClient.invoke(testInstance.deviceIdentityArray[i].getDeviceId(), expectedMethodName, 200l, 200l, null);

            // No need to wait for the device to receive the method invocation since the service client call does that already
            assertTrue("Device method callback never fired on device", deviceMethodCallback.deviceMethodCallbackFired);
            assertTrue("Device method callback fired, but unexpected method name was received", deviceMethodCallback.expectedMethodReceived);
        }

        testInstance.multiplexingClient.close();
    }

    private class DeviceMethodCallback implements com.microsoft.azure.sdk.iot.device.DeviceTwin.DeviceMethodCallback
    {
        public boolean deviceMethodCallbackFired = false;
        public boolean expectedMethodReceived = false;

        final String expectedMethodName;

        public DeviceMethodCallback(String expectedMethodName)
        {
            this.expectedMethodName = expectedMethodName;
        }

        @Override
        public DeviceMethodData call(String methodName, Object methodData, Object context) {
            deviceMethodCallbackFired = true;
            if (methodName.equals(expectedMethodName))
            {
                expectedMethodReceived = true;
            }

            return new DeviceMethodData(200, null);
        }
    }

    class TwinPropertyCallBackImpl implements TwinPropertyCallBack
    {
        String expectedKey;
        String expectedValue;

        public boolean receivedCallback = false;
        public boolean receivedExpectedKey = false;
        public boolean receivedExpectedValue = false;

        public String actualKey;
        public String actualValue;

        public TwinPropertyCallBackImpl(String expectedKey, String expectedValue)
        {
            this.expectedKey = expectedKey;
            this.expectedValue = expectedValue;
        }

        @Override
        public void TwinPropertyCallBack(Property property, Object context)
        {
            actualKey = property.getKey();
            if (actualKey.equals(expectedKey))
            {
                receivedExpectedKey = true;

                actualValue = property.getValue().toString();
                if (actualValue.equals(expectedValue))
                {
                    receivedExpectedValue = true;
                }
            }

            receivedCallback = true;
        }
    }

    @Test
    @StandardTierHubOnlyTest
    public void testTwin() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);
        testInstance.multiplexingClient.open();

        DeviceTwin deviceTwinServiceClient = DeviceTwin.createFromConnectionString(iotHubConnectionString, DeviceTwinClientOptions.builder().httpReadTimeout(0).build());

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            String expectedPropertyKey = UUID.randomUUID().toString();
            String expectedPropertyValue = UUID.randomUUID().toString();
            TwinPropertyCallBackImpl twinPropertyCallBack = new TwinPropertyCallBackImpl(expectedPropertyKey, expectedPropertyValue);
            EventCallback twinEventCallback = new EventCallback(IotHubStatusCode.OK);
            Success twinStarted = new Success();
            testInstance.deviceClientArray[i].startDeviceTwin(twinEventCallback, twinStarted, twinPropertyCallBack, null);

            long startTime = System.currentTimeMillis();
            while (!twinStarted.wasCallbackFired())
            {
                Thread.sleep(200);

                if (System.currentTimeMillis() - startTime > TWIN_SUBSCRIBE_TIMEOUT_MILLIS)
                {
                    fail("Timed out waiting for twin to start");
                }
            }

            assertTrue("Failed to start twin. Unexpected status code " + twinStarted.getCallbackStatusCode(), twinStarted.getResult());

            // Testing subscribing to desired properties
            Map<Property, Pair<TwinPropertyCallBack, Object>> onDesiredPropertyChange = new HashMap<>();
            onDesiredPropertyChange.put(new Property(expectedPropertyKey, null), new Pair<>(twinPropertyCallBack, null));
            testInstance.deviceClientArray[i].subscribeToTwinDesiredProperties(onDesiredPropertyChange);

            Thread.sleep(MAXIMUM_TIME_TO_WAIT_FOR_DESIRED_PROPERTY_SUBSCRIPTION_ACKNOWLEDGEMENT);

            // Send desired property update to multiplexed device
            DeviceTwinDevice serviceClientTwin = new DeviceTwinDevice(testInstance.deviceIdentityArray[i].getDeviceId());
            Set<com.microsoft.azure.sdk.iot.service.devicetwin.Pair> desiredProperties = new HashSet<>();
            desiredProperties.add(new com.microsoft.azure.sdk.iot.service.devicetwin.Pair(expectedPropertyKey, expectedPropertyValue));
            serviceClientTwin.setDesiredProperties(desiredProperties);
            deviceTwinServiceClient.updateTwin(serviceClientTwin);

            startTime = System.currentTimeMillis();
            while (!twinPropertyCallBack.receivedCallback)
            {
                Thread.sleep(200);

                if (System.currentTimeMillis() - startTime > DESIRED_PROPERTY_CALLBACK_TIMEOUT_MILLIS)
                {
                    fail("Timed out waiting for desired property callback to fire");
                }
            }

            assertTrue("Desired property callback fired with unexpected key. Expected " + expectedPropertyKey + " but was " + twinPropertyCallBack.actualKey, twinPropertyCallBack.receivedExpectedKey);
            assertTrue("Desired property callback fired with unexpected value. Expected " + expectedPropertyValue + " but was " + twinPropertyCallBack.actualValue, twinPropertyCallBack.receivedExpectedValue);

            // Testing sending reported properties
            String expectedReportedPropertyValue = expectedPropertyValue + "-reported";
            Set<Property> reportedProperties = new HashSet<>();
            reportedProperties.add(new Property(expectedPropertyKey, expectedReportedPropertyValue));
            testInstance.deviceClientArray[i].sendReportedProperties(reportedProperties);

            Thread.sleep(MAXIMUM_TIME_TO_WAIT_FOR_REPORTED_PROPERTY_ACKNOWLEDGEMENT);

            // Verify that the new reported property value can be seen from the service client
            deviceTwinServiceClient.getTwin(serviceClientTwin);

            Set<com.microsoft.azure.sdk.iot.service.devicetwin.Pair> retrievedReportedProperties = serviceClientTwin.getReportedProperties();
            assertEquals(1, retrievedReportedProperties.size());
            com.microsoft.azure.sdk.iot.service.devicetwin.Pair retrievedReportedPropertyPair = retrievedReportedProperties.iterator().next();
            assertTrue(retrievedReportedPropertyPair.getKey().equalsIgnoreCase(expectedPropertyKey));
            String actualReportedPropertyValue = retrievedReportedPropertyPair.getValue().toString();
            assertEquals(expectedReportedPropertyValue, actualReportedPropertyValue);
        }
    }

    class ConnectionStatusChangeTracker implements IotHubConnectionStatusChangeCallback
    {
        public boolean isOpen = false;

        // flags that, at some point, this device went into disconnected retrying state. May have recovered, though
        public boolean wentDisconnectedRetrying = false;

        public boolean clientClosedGracefully = false;

        @Override
        public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason, Throwable throwable, Object callbackContext)
        {
            if (status == IotHubConnectionStatus.CONNECTED)
            {
                isOpen = true;
            }
            else if (status == IotHubConnectionStatus.DISCONNECTED)
            {
                isOpen = false;

                // client may close due to unexpected exception. For our test purposes, we want to validate that this callback gets fired
                // with reason CLIENT_CLOSE since that is the happy-path close status
                if (statusChangeReason == IotHubConnectionStatusChangeReason.CLIENT_CLOSE)
                {
                    clientClosedGracefully = true;
                }
            }
            else if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING)
            {
                wentDisconnectedRetrying = true;
                isOpen = false;
            }
        }
    }

    @Test
    @StandardTierHubOnlyTest
    public void registerClientAfterOpen() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);

        // Unregister one client so that it can be registered after the open call
        DeviceClient clientToRegisterAfterOpen = testInstance.deviceClientArray[DEVICE_MULTIPLEX_COUNT - 1];
        testInstance.multiplexingClient.unregisterDeviceClient(clientToRegisterAfterOpen);

        testInstance.multiplexingClient.open();

        ConnectionStatusChangeTracker connectionStatusChangeTracker = new ConnectionStatusChangeTracker();
        clientToRegisterAfterOpen.registerConnectionStatusChangeCallback(connectionStatusChangeTracker, null);

        testInstance.multiplexingClient.registerDeviceClient(clientToRegisterAfterOpen);

        assertDeviceSessionOpens(connectionStatusChangeTracker, DEVICE_SESSION_OPEN_TIMEOUT);

        testSendingMessageFromMultiplexedClient(clientToRegisterAfterOpen);
    }

    @Test
    @StandardTierHubOnlyTest
    public void unregisterClientAfterOpen() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);

        // pick the 0th client to unregister after open. This will help make sure we don't have any dependencies on the 0th registered client
        DeviceClient clientToUnregisterAfterOpen = testInstance.deviceClientArray[0];

        ConnectionStatusChangeTracker connectionStatusChangeTracker = new ConnectionStatusChangeTracker();
        clientToUnregisterAfterOpen.registerConnectionStatusChangeCallback(connectionStatusChangeTracker, null);

        testInstance.multiplexingClient.open();

        assertDeviceSessionOpens(connectionStatusChangeTracker, DEVICE_SESSION_OPEN_TIMEOUT);

        testInstance.multiplexingClient.unregisterDeviceClient(clientToUnregisterAfterOpen);

        assertDeviceSessionClosesGracefully(connectionStatusChangeTracker, DEVICE_SESSION_CLOSE_TIMEOUT);

        // start index from 1 since the 0th client was deliberately unregistered
        for (int i = 1; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            testSendingMessageFromMultiplexedClient(testInstance.deviceClientArray[i]);
        }

        // verify that unregistered clients don't attempt to send messages on the active multiplexed connection after unregistration
        boolean exceptionThrown;
        try
        {
            testInstance.deviceClientArray[0].sendEventAsync(new Message("This message shouldn't be sent"), new EventCallback(IotHubStatusCode.OK_EMPTY), null);
            exceptionThrown = false;
        }
        catch (UnsupportedOperationException e)
        {
            exceptionThrown = true;
        }

        assertTrue("Expected exception to be thrown when sending a message from an unregistered client", exceptionThrown);
    }

    @Test
    public void multiplexedConnectionRecoversFromDeviceSessionDrops() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);
        ConnectionStatusChangeTracker[] connectionStatusChangeTrackers = new ConnectionStatusChangeTracker[DEVICE_MULTIPLEX_COUNT];

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            connectionStatusChangeTrackers[i] = new ConnectionStatusChangeTracker();
            testInstance.deviceClientArray[i].registerConnectionStatusChangeCallback(connectionStatusChangeTrackers[i], null);
        }

        testInstance.multiplexingClient.open();

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            assertTrue("Multiplexing client opened successfully, but connection status change callback didn't execute.", connectionStatusChangeTrackers[i].isOpen);
        }

        // For each multiplexed device, use fault injection to drop the session and see if it can recover, one device at a time
        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            Message errorIjectionMessage = ErrorInjectionHelper.amqpsSessionDropErrorInjectionMessage(1, 10);
            testSendingMessageFromMultiplexedClient(testInstance.deviceClientArray[i], errorIjectionMessage, "Timed out waiting for error injection message to be acknowledged");

            // Now that error injection message has been sent, need to wait for the device session to drop
            assertDeviceCallsbackDisconnectedRetrying(connectionStatusChangeTrackers[i]);

            // Next, the faulted device should eventually recover
            assertDeviceSessionOpens(connectionStatusChangeTrackers[i], FAULT_INJECTION_RECOVERY_TIMEOUT_MILLIS);

            for (int j = i + 1; j < DEVICE_MULTIPLEX_COUNT; j++)
            {
                // devices above index i have not been deliberately faulted yet, so make sure they haven't seen a DISCONNECTED_RETRYING event yet.
                assertFalse("Multiplexed device that hasn't been deliberately faulted yet saw an unexpected DISCONNECTED_RETRYING connection status callback", connectionStatusChangeTrackers[j].wentDisconnectedRetrying);
            }

            // Try to send a message over the now-recovered device session
            testSendingMessageFromMultiplexedClient(testInstance.deviceClientArray[i]);
        }

        // double check that the recovery of any particular device did not cause a device earlier in the array to lose connection
        testSendingMessagesFromMultiplexedClients(testInstance.deviceClientArray);

        testInstance.multiplexingClient.close();

        assertMultiplexedDevicesClosedGracefully(connectionStatusChangeTrackers);
    }

    @Test
    public void multiplexedConnectionRecoversFromTcpConnectionDrop() throws Exception
    {
        testInstance.setup(DEVICE_MULTIPLEX_COUNT);
        ConnectionStatusChangeTracker[] connectionStatusChangeTrackers = new ConnectionStatusChangeTracker[DEVICE_MULTIPLEX_COUNT];

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            connectionStatusChangeTrackers[i] = new ConnectionStatusChangeTracker();
            testInstance.deviceClientArray[i].registerConnectionStatusChangeCallback(connectionStatusChangeTrackers[i], null);
        }

        testInstance.multiplexingClient.open();

        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            assertTrue("Multiplexing client opened successfully, but connection status change callback didn't execute.", connectionStatusChangeTrackers[i].isOpen);
        }

        Message errorIjectionMessage = ErrorInjectionHelper.tcpConnectionDropErrorInjectionMessage(1, 10);
        testSendingMessageFromMultiplexedClient(testInstance.deviceClientArray[0], errorIjectionMessage, "Timed out waiting for error injection message to be acknowledged");

        // Now that error injection message has been sent, need to wait for the device session to drop
        assertDeviceCallsbackDisconnectedRetrying(connectionStatusChangeTrackers[0]);

        // For each multiplexed device, use fault injection to drop the session and see if it can recover, one device at a time
        for (int i = 0; i < DEVICE_MULTIPLEX_COUNT; i++)
        {
            // Next, the faulted device should eventually recover
            assertDeviceSessionOpens(connectionStatusChangeTrackers[i], FAULT_INJECTION_RECOVERY_TIMEOUT_MILLIS);

            // Try to send a message over the now-recovered device session
            testSendingMessageFromMultiplexedClient(testInstance.deviceClientArray[i]);
        }

        // double check that the recovery of any particular device did not cause a device earlier in the array to lose connection
        testSendingMessagesFromMultiplexedClients(testInstance.deviceClientArray);

        testInstance.multiplexingClient.close();

        assertMultiplexedDevicesClosedGracefully(connectionStatusChangeTrackers);
    }

    private static void assertMultiplexedDevicesClosedGracefully(ConnectionStatusChangeTracker[] connectionStatusChangeTrackers)
    {
        for (ConnectionStatusChangeTracker connectionStatusChangeTracker : connectionStatusChangeTrackers)
        {
            assertTrue("Multiplexed device never reported closing as expected", connectionStatusChangeTracker.clientClosedGracefully);
        }
    }

    private static void assertDeviceSessionOpens(ConnectionStatusChangeTracker connectionStatusChangeTracker, int timeoutMillis) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (!connectionStatusChangeTracker.isOpen)
        {
            Thread.sleep(200);

            if (System.currentTimeMillis() - startTime > timeoutMillis)
            {
                fail("Timed out waiting for faulted device to become reconnected");
            }
        }
    }

    private static void assertDeviceSessionClosesGracefully(ConnectionStatusChangeTracker connectionStatusChangeTracker, int timeoutMillis) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (connectionStatusChangeTracker.isOpen)
        {
            Thread.sleep(200);

            if (System.currentTimeMillis() - startTime > timeoutMillis)
            {
                fail("Timed out waiting for faulted device to become reconnected");
            }
        }

        assertTrue("Device session was closed, but did not close gracefully", connectionStatusChangeTracker.clientClosedGracefully);
    }

    private static void assertDeviceCallsbackDisconnectedRetrying(ConnectionStatusChangeTracker connectionStatusChangeTracker) throws InterruptedException
    {
        long startTime = System.currentTimeMillis();
        while (!connectionStatusChangeTracker.wentDisconnectedRetrying)
        {
            Thread.sleep(200);

            if (System.currentTimeMillis() - startTime > FAULT_INJECTION_TIMEOUT_MILLIS)
            {
                fail("Timed out waiting for device to become disconnected-retrying");
            }
        }
    }
}
