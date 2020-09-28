package com.microsoft.azure.sdk.iot.device;

import com.microsoft.azure.sdk.iot.device.transport.amqps.IoTHubConnectionType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * A client for creating multiplexed connections to IoT Hub. A multiplexed connection allows for multiple device clients
 * to communicate to the service through a single AMQPS connection.
 * <p>
 * A given AMQPS connection requires a TLS connection, so multiplexing may be worthwhile if you want to limit the number
 * of TLS connections needed to connect multiple device clients to IoT Hub.
 * <p>
 * A given multiplexing client also has a fixed amount of worker threads regardless of how many device clients are
 * being multiplexed. Comparatively, every non-multiplexed device client instance has its own set of worker
 * threads. Multiplexing may be worthwhile if you want fewer worker threads.
 * <p>
 * Only AMQPS and AMQPS_WS support multiplexing, and only symmetric key authenticated devices can be multiplexed.
 * <p>
 * {@link ModuleClient} instances cannot be multiplexed.
 */
@Slf4j
public class MultiplexingClient
{
    public static long SEND_PERIOD_MILLIS = 10L;
    public static long RECEIVE_PERIOD_MILLIS = 10L;

    private DeviceIO deviceIO;
    private ArrayList<DeviceClient> deviceClientList;
    private IotHubClientProtocol protocol;
    private ProxySettings proxySettings;

    /**
     * Instantiate a new MultiplexingClient that will establish a multiplexed connection through a proxy.
     *
     * @param protocol The transport protocol that this client will build the multiplexed connection on. Must be either
     *                 {@link IotHubClientProtocol#AMQPS} or {@link IotHubClientProtocol#AMQPS_WS}.
     */
    public MultiplexingClient(IotHubClientProtocol protocol)
    {
        this(protocol, null);
    }

    /**
     * Instantiate a new MultiplexingClient that will establish a multiplexed connection through a proxy.
     *
     * @param protocol The transport protocol that this client will build the multiplexed connection on. Must be
     * {@link IotHubClientProtocol#AMQPS_WS} since using {@link IotHubClientProtocol#AMQPS} does not support proxies.
     * @param proxySettings The proxy settings that this client will use.
     */
    public MultiplexingClient(IotHubClientProtocol protocol, ProxySettings proxySettings)
    {
        switch (protocol)
        {
            case AMQPS:
            case AMQPS_WS:
                break;
            default:
                throw new IllegalArgumentException("Multiplexing is only supported for AMQPS and AMQPS_WS");
        }

        this.deviceClientList = new ArrayList<>();
        this.protocol = protocol;
        this.proxySettings = proxySettings;
    }

    /**
     * Opens this multiplexing client. At least one device client must be registered prior to calling this.
     * <p>
     * This call behaves synchronously, so if it returns without throwing, then all registered device clients were
     * successfully opened.
     * <p>
     * If this client is already open, then this method will do nothing.
     * <p>
     * @throws IOException If any IO errors occur while opening the multiplexed connection.
     */
    public void open() throws IOException
    {
        if (deviceClientList.size() < 1)
        {
            throw new UnsupportedOperationException("Must register at least one device client before opening a multiplexed connection");
        }

        log.info("Opening multiplexing client");
        this.deviceIO.open();
        log.info("Successfully opened multiplexing client");
    }

    /**
     * Close this multiplexing client. This will close all active device sessions as well as the AMQP connection.
     * <p>
     * If this client is already closed, then this method will do nothing.
     * <p>
     * Once closed, this client can be re-opened. It will preserve the previously registered device clients.
     * <p>
     * @throws IOException If any exception occurs while closing the connection.
     */
    public void close() throws IOException
    {
        log.info("Closing multiplexing client");
        for (DeviceClient deviceClient : this.deviceClientList)
        {
            deviceClient.closeFileUpload();
        }

        if (this.deviceIO != null)
        {
            this.deviceIO.multiplexClose();
        }

        log.info("Successfully closed multiplexing client");
    }

    /**
     * Add a device to this multiplexing client. This method may be called before or after opening the multiplexed
     * connection, but will behave differently depending on when it was called.
     * <p>
     * Up to 1000 devices can be registered on a multiplexed AMQPS connection, and up to 500 devices can be registered on a
     * multiplexed AMQPS_WS connection.
     * <p>
     * If the multiplexing client is already open, then this device client will automatically
     * be opened, too. If the multiplexing client is not open yet, then this device client will not be opened until
     * {@link MultiplexingClient#open()} is called.
     * <p>
     * If the multiplexed connection is already open, then this is an asynchronous operation. You can track the state of your
     * device session using the {@link DeviceClient#registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback, Object)}.
     * That callback will execute once your device session has successfully been added to the existing multiplexed connection and
     * is ready to send telemetry.
     * <p>
     * Any proxy settings set to the provided device client will be overwritten by the proxy settings of this multiplexing client.
     * <p>
     * The registered device client instance must use the same transport protocol (AMQPS or AMQPS_WS) that this multiplexing client uses.
     * <p>
     * The registered device client may have its own retry policy and its own SAS token expiry time, separate from every other multiplexed device.
     * <p>
     * The registered device client must use symmetric key based authentication.
     * <p>
     * The registered device client must belong to the same IoT Hub as all previously registered device clients.
     * <p>
     * If this device client is already registered, then this method will throw a {@link IllegalStateException}
     * <p>
     * @param deviceClient The device client to associate with this multiplexing client.
     */
    public void registerDeviceClient(DeviceClient deviceClient)
    {
        Objects.requireNonNull(deviceClient);

        for (DeviceClient currentClient : deviceClientList)
        {
            if (currentClient.getConfig().getDeviceId().equals(deviceClient.getConfig().getDeviceId()))
            {
                throw new IllegalStateException("Cannot register device client because a device client for that device identity has already been registered");
            }
        }

        if (deviceClient.getConfig().getAuthenticationType() != DeviceClientConfig.AuthType.SAS_TOKEN)
        {
            throw new UnsupportedOperationException("Can only register a device client if it uses SAS token based authentication");
        }

        if (deviceClient.getConfig().getProtocol() != this.protocol)
        {
            throw new UnsupportedOperationException("Cannot register a device client that uses a different transport protocol than this multiplexing client uses");
        }

        if (this.protocol == IotHubClientProtocol.AMQPS && this.deviceClientList.size() > 1000)
        {
            throw new UnsupportedOperationException("Multiplexed connections over AMQPS only support up to 1000 devices");
        }

        // Typically client side validation is duplicate work, but IoT Hub doesn't give a good error message when closing the
        // AMQPS_WS connection so this is the only way that users will know about this limit
        if (this.protocol == IotHubClientProtocol.AMQPS_WS && this.deviceClientList.size() > 500)
        {
            throw new UnsupportedOperationException("Multiplexed connections over AMQPS_WS only support up to 500 devices");
        }

        if (this.deviceClientList.size() > 1 && !this.deviceClientList.get(0).getConfig().getIotHubHostname().equals(deviceClient.getConfig().getIotHubHostname()))
        {
            throw new UnsupportedOperationException("Multiplexed device clients must connect to the same host name");
        }

        // The first device to be registered will cause this client to build the IO layer with its configuration
        if (this.deviceIO == null)
        {
            log.debug("Creating DeviceIO layer for multiplexing client since this is the first registered device");
            this.deviceIO = new DeviceIO(deviceClient.getConfig(), SEND_PERIOD_MILLIS, RECEIVE_PERIOD_MILLIS);
        }

        deviceClient.setDeviceIO(this.deviceIO);
        deviceClient.getConfig().setProxy(this.proxySettings);
        deviceClient.setConnectionType(IoTHubConnectionType.USE_MULTIPLEXING_CLIENT);
        this.deviceClientList.add(deviceClient);

        // if the device IO hasn't been created yet, then this client will be registered once it is created.
        log.info("Registering device {} to multiplexing client", deviceClient.getConfig().getDeviceId());
        this.deviceIO.registerMultiplexedDeviceClient(deviceClient.getConfig());
    }

    /**
     * Remove a device client from this multiplexing client. This method may be called before or after opening the
     * multiplexed connection, but will behave differently depending on when it was called.
     * <p>
     * If the multiplexed connection is already open, then this call will close the AMQP device session associated with
     * this device, but it will not close any other registered device sessions or the multiplexing client itself.
     * <p>
     * If the multiplexed connection is already open, then this is an asynchronous operation and you can track the state of your
     * device session using the {@link DeviceClient#registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback, Object)}.
     * <p>
     * If the multiplexed connection is already open, then at least one device client must be registered at any given time.
     * Because of this, this method will throw an {@link IllegalStateException} if it attempts to remove the last device client.
     * <p>
     * @param deviceClient The device client to unregister from this multiplexing client.
     */
    public void unregisterDeviceClient(DeviceClient deviceClient)
    {
        Objects.requireNonNull(deviceClient);

        if (deviceClientList.size() <= 1)
        {
            throw new IllegalStateException("Cannot unregister the last device. At least one device client must be registered to this multiplexing client.");
        }

        log.info("Unregistering device {} from multiplexing client", deviceClient.getConfig().getDeviceId());
        this.deviceClientList.remove(deviceClient);
        this.deviceIO.unregisterMultiplexedDeviceClient(deviceClient.getConfig());
        deviceClient.setDeviceIO(null);
    }
}
