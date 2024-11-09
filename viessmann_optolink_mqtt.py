import sys
import time
import telnetlib
import re
import argparse
import logging
import socket
import paho.mqtt.client as mqtt
from threading import Lock

from __version import __version__
from homeassistant_auto_discovery import (
    HassDevice,
    HassSensor, HassBinarySensor, HassSelect,
    HassNumber,
)

# 192.168.1.250:30080


LOG = logging.getLogger("optolink2mqtt")

DEVICE = HassDevice("vcontrold", "Viessmann Optolink",
                    __version__, "unknown", "Viessmann")


class VitoConnection(object):
    def __init__(self, telnet: telnetlib.Telnet, cmd: str) -> None:
        self.telnet_client = telnet
        self.cmd = cmd
        self.__lock = Lock()

    def set_telnet_client(self, telnet):
        self.telnet_client = telnet

    def __read(self, until: str) -> str:
        if isinstance(until, str):
            until = str.encode(until)
        _resp = self.telnet_client.read_until(until, timeout=10).decode().strip()
        return _resp.replace("vctrld>", "")  # remove possible vcontrold cli "header"

    def __read_line(self) -> str:
        return self.__read('\n')

    def __send(self, cmd: str = None, value: str = None) -> None:
        if not isinstance(cmd, str):
            raise ValueError("command must be string")
        if value is not None:
            if not isinstance(cmd, str):
                raise ValueError("value must be a string")
            cmd = f"set-{cmd} {value}"
        self.telnet_client.write(str.encode(cmd + '\n'))

    def query(self) -> str:
        if not self.cmd:
            return
        self.__lock.acquire()
        # self.__read("vctrld>")
        self.telnet_client.read_very_eager()
        self.__send(self.cmd)
        resp = self.__read_line()
        self.__lock.release()
        return resp

    def set_value(self, value: str):
        if not self.cmd:
            return
        self.__lock.acquire()
        # self.__read("vctrld>")
        self.telnet_client.read_very_eager()
        self.__send(self.cmd, value)
        resp = self.__read_line()
        self.__lock.release()
        if "OK" not in resp:
            raise ValueError(f"Unable to write the value {value}: '{resp}'")


class VitoElementBinary(HassBinarySensor):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd)
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        val = self.conn.query()
        return ["ON", "OFF"][val in ["0", "Off"]]

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementTemperature(HassSensor):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd,
                         device_class="temperature", unit_of_measurement="°C",
                         state_class="measurement")
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        val = self.conn.query()
        resp = re.search(r"(-*\d+\.\d+).*", val)
        if resp:
            return "%.1f" % float(resp.group(1))
        self.error_count += 1
        return None

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementTemperatureWritable(HassNumber):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet,
                 min: int, max: int) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd,
                         device_class="temperature", unit_of_measurement="°C",
                         state_class="measurement", min=min, max=max)
        self.conn = VitoConnection(telnet, cmd)
        self.current_value = None

    @property
    def value(self) -> None:
        val = self.conn.query()
        resp = re.search(r"(-*\d+\.\d+).*", val)
        if resp:
            _val = float(resp.group(1))
            self.current_value = _val
            return "%.1f" % _val
        self.error_count += 1
        return None

    def set_value(self, value: str):
        LOG.info(f"{self.name}: set_value called with arg: '{value}'")
        if not value.isnumeric():
            return
        value = int(value)
        if self.current_value is None or value == self.current_value:
            return
        self.conn.set_value(value)
        self.current_value = value
        LOG.info(f"{self.name}: value written successfully")

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementPercent(HassSensor):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd,
                         device_class="", unit_of_measurement="%",
                         state_class="measurement")
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        val = self.conn.query()
        resp = re.search(r"(-*\d+\.\d+).*", val)
        if resp:
            return "%d" % int(float(resp.group(1)))
        self.error_count += 1
        return None

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementNumber(HassSensor):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet,
                 slope: bool = False) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd,
                         icon="mdi:chart-bell-curve-cumulative" if slope else "",
                         category="diagnostic")
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        val = self.conn.query()
        resp = re.search(r"(-*\d+\.\d+).*", val)
        if resp:
            return "%.1f" % float(resp.group(1))
        self.error_count += 1
        return None

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementSelect(HassSelect):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet,
                 options: dict = {}) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd,
                         category="config", options=list(options.keys()), cmd=True)
        self.conn = VitoConnection(telnet, cmd)
        self.options: dict = options
        self.current_value = None

    @property
    def value(self) -> None:
        val = self.conn.query()
        for key, value in self.options.items():
            if value == val:
                self.current_value = key
                return key
        self.error_count += 1
        return ""

    def set_value(self, value: str):
        _new_val = self.options.get(value)
        if _new_val is None:
            LOG.error(f"{self.name}: Invalid value '{value}'")
            return
        LOG.info(f"{self.name}: set_value called with arg: '{value}' -> {_new_val}")
        if _new_val != self.current_value:
            self.conn.set_value(_new_val)
            self.current_value = _new_val

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementText():
    def __init__(self, cmd: str, telnet: telnetlib.Telnet) -> None:
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        return self.conn.query()


class VitoElementTextWrite(VitoElementText):
    pass
    # @VitoElementText.value.setter
    # def value(self, value) -> None:
    #     if not isinstance(value, str):
    #         raise ValueError("Only string is supported")
    #     # TODO: write text...


class ClientBase(object):
    def run(self, mqtt: mqtt.Client) -> None:
        raise NotImplementedError("run method is not implemented!")


class VControldClient(ClientBase):

    class ConnectionFailure(Exception):
        pass

    def __init__(self, host, port, prefix, logger: logging.Logger):
        super().__init__()
        DEVICE.set_prefix(prefix)
        self.logger = logger.getChild("vcontrold")
        self.host = host
        self.port = port
        self.telnet_client = telnet = None  # telnetlib.Telnet(host, port)
        self.sensors = [
            # Sensors
            VitoElementTemperature("Water top temperature", "DHW-TopTemp", telnet),
            VitoElementTemperature("Water bottom temperature", "DHW-BottomTemp", telnet),
            VitoElementTemperature("Outdoor temperature", "TempOutdoor", telnet),
            VitoElementTemperature("Room temperature, party", "RoomTempPartyMode", telnet),
            VitoElementTemperature("Room temperature, reduced, ", "RoomTempReduced", telnet),
            VitoElementTemperatureWritable("Room temperature, normal", "RoomTempNormal", telnet, 14, 20),
            VitoElementTemperature("Heating", "Heating", telnet),
            VitoElementTemperature("Circuit temperature, primary, flow", "PrimaryCircuitFlowTemp", telnet),
            VitoElementTemperature("Circuit temperature, primary, return", "PrimaryCircuitReturnTemp", telnet),
            VitoElementTemperature("Circuit temperature, secondary, flow", "SecondaryCircuitFlowTemp", telnet),
            VitoElementTemperature("Circuit temperature, secondary, return", "SecondaryCircuitReturnTemp", telnet),
            VitoElementTemperatureWritable("DHW Setpoint Normal", "DHW-Setpoint", telnet, 20, 60),
            VitoElementTemperatureWritable("DHW Setpoint High", "DHW-Setpoint2", telnet, 20, 60),
            VitoElementTemperatureWritable("DHW Heat Pump Hysteresis", "DHW-HeatPumpHysteresis", telnet, 1, 60),
            VitoElementTemperatureWritable("DHW Booster Hysteresis", "DHW-BoosterHysteresis", telnet, 1, 60),
            VitoElementBinary("Compressor", "Compressor", telnet),
            VitoElementBinary("Pump, primary", "PrimaryPump", telnet),
            VitoElementBinary("Pump, secondary", "SecondaryPump", telnet),
            VitoElementBinary("Pump, DHW", "DHWPump", telnet),
            VitoElementBinary("Pump, DHW circulation", "DHWCirculationPump", telnet),
            VitoElementPercent("Pump, DHW PWM", "DHW-LoadPumpPwm", telnet),

            # Diagnostic
            VitoElementNumber("Heating Curve Slope", "HeatingCurveSlope", telnet, slope=True),
            VitoElementNumber("Heating Curve Level", "HeatingCurveLevel", telnet, slope=True),
            VitoElementNumber("Heating COP", "COP-Heating", telnet),
            VitoElementNumber("DHW COP", "COP-DHW", telnet),
            VitoElementNumber("Total COP", "SCOP", telnet),

            # Config
            VitoElementSelect("Operation Mode", "OpMode", telnet,
                              {"Heating and DHW": "Heating and DHW",
                               "DHW Only": "DHW only",
                               "Reduced": "Continuous Reduced",
                               "Normal": "Continuous Normal",
                               "Standby": "Standby",
                               "Shutdown": "Shutdown",
                               }),
        ]
        self._vito_device = VitoElementText("DeviceType", self.telnet_client)
        self.__connect_telnet_client()

    def __connect_telnet_client(self):
        self.logger.info("Connecting to telnet host...")
        telnet = self.telnet_client
        if telnet:
            telnet.close()
        telnet = telnetlib.Telnet(self.host, self.port, timeout=20)
        time.sleep(.5)  # wait old data to be received
        old = telnet.read_eager()  # empty rx buffer
        if old:
            self.logger.debug(f"Old data: '{old}'")
        self.telnet_client = telnet
        # update clients
        self._vito_device.conn.set_telnet_client(telnet)
        for sensor in self.sensors:
            sensor.conn.set_telnet_client(telnet)

    def get_device_type(self):
        model = self._vito_device.value
        # Check the model response. Should be something like 'Vitocal 333-G ID=204B Protokoll:P300'
        resp = re.search(r"(.*) ID=([0-9a-fA-F]+) Protokoll:(.*)", model)
        if not resp:
            raise self.ConnectionFailure("Unable to fetch the device type")
        return resp.group(1)

    def version(self) -> None:
        self.logger.debug("Fetching the version...")
        test = VitoElementText("version", self.telnet_client)
        self.logger.info(f"vcontrold: {test.value}")
        self.logger.debug("Fetching a device type...")
        model = self.get_device_type()
        DEVICE.set_model(model)
        self.logger.info(f"Connected to device: {model}")

    def connected(self, mqtt: mqtt.Client, availability_topic: str = "") -> str:
        """
        Called when MQTT gets connected. Purpose is to publish configuration topics
        and register write topics
        """
        for sensor in self.sensors:
            config = sensor.get_config(availability_topic)
            # retain config
            mqtt.publish(*config, retain=True)
            _set_topic = sensor.get_topic_set()
            _cb = getattr(sensor, "set_value", None)
            if _set_topic and _cb:
                mqtt.register_subscription(_set_topic, _cb)
            # self.logger.debug(f"Publish: {config}")

    def run(self, mqtt) -> list:
        try:
            self.get_device_type()  # to make sure the connection is ok
        except self.ConnectionFailure:
            self.__connect_telnet_client()
            return  # Will reschedule soon...
        for sensor in self.sensors:
            update = sensor.value_with_topic()
            mqtt.publish(*update, retain=False)
            # self.logger.debug(f"Publish: {update}")


class MqttClient(mqtt.Client):
    class RestartRequired(Exception):
        pass

    def __init__(self, client_id=None, transport="tcp", reconnect_on_failure=True,
                 prefix='homeassistant', interval=30):
        super().__init__(client_id=client_id, transport=transport,
                         reconnect_on_failure=reconnect_on_failure)
        self._clients: ClientBase = []
        self._subscriptions = {}

        self.prefix = prefix
        self.interval = interval
        self.connected = False
        self.cfg_topic_prefix = f'{prefix}/optolink'
        self.availability_topic = f'{self.cfg_topic_prefix}/state'

        self.on_connect = self.__on_connect_callback
        self.on_message = self.__on_message_callback
        self.on_disconnect = self.__on_disconnect_callback
        self.on_subscribe = self.__on_subscribe_callback

        self.will_set(topic=self.availability_topic, qos=1, retain=True, payload="offline")

    def client_add(self, client: ClientBase):
        if client not in self._clients:
            self._clients.append(client)

    def register_subscription(self, topic: str, cb, qos: int = 1):
        if not callable(cb):
            LOG.error(f"Trying to register topic '{topic}' for not callable!")
            return
        if topic not in self._subscriptions:
            self._subscriptions[topic] = cb
            self.subscribe(topic, qos=qos)

    def __force_update_received(self, msg: str):
        if msg.lower() == 'true':
            LOG.info('MQTT received: Forced update triggered')
            self.run_clients()

    def __update_interval_received(self, msg: str):
        if msg.isnumeric():
            __newInterval = int(msg)
            # TODO: range check...MQTT-Server
            self.interval = __newInterval
            LOG.info('MQTT received: Interval updated: %ds' % __newInterval)
        else:
            LOG.error(f'MQTT received: Interval is not a number: {msg}')
        # publish new/old value
        self.publish(topic=f'{self.cfg_topic_prefix}/updateInterval_s', qos=1, retain=False,
                     payload=self.interval)

    def __on_connect_callback(self, mqttc, obj, flags, rc):
        del mqttc  # unused
        del obj  # unused
        del flags  # unused

        if rc == 0:
            LOG.info('Connected to MQTT broker')
            # Subscribe for topis
            topic = f'{self.cfg_topic_prefix}/forcedUpdate_write'
            self.register_subscription(topic, self.__force_update_received, qos=2)

            topic = f'{self.cfg_topic_prefix}/updateInterval_s'
            self.publish(topic=topic, qos=1, retain=True, payload=self.interval)
            self.register_subscription(topic + '_write', self.__update_interval_received)

            self.set_connected(True)
            time.sleep(0.2)
            self.run_clients()
        elif rc == 1:
            LOG.error('Could not connect (%d): incorrect protocol version', rc)
        elif rc == 2:
            LOG.error('Could not connect (%d): invalid client identifier', rc)
        elif rc == 3:
            LOG.error('Could not connect (%d): server unavailable. Retrying in 10 seconds', rc)
            time.sleep(10)
            self.reconnect()
        elif rc == 4:
            LOG.error('Could not connect (%d): bad username or password', rc)
        elif rc == 5:
            LOG.error('Could not connect (%d): not authorised', rc)
        else:
            print('Could not connect: %d', rc, file=sys.stderr)
            sys.exit(1)

    def __on_disconnect_callback(self, client, userdata, rc):
        del client
        del userdata

        if rc == 0:
            LOG.info('MQTT Client successfully disconnected')
        else:
            LOG.info('MQTT Client unexpectedly disconnected (%d), trying to reconnect', rc)
            while True:
                try:
                    self.reconnect()
                    break
                except ConnectionRefusedError as e:
                    LOG.error('MQTT: Could not reconnect to %s, will retry in 10 seconds', e)
                except socket.timeout:
                    LOG.error('MQTT: Could not reconnect due to timeout, will retry in 10 seconds')
                except OSError as e:
                    LOG.error('MQTT: Could not reconnect to %s, will retry in 10 seconds', e)
                finally:
                    time.sleep(10)

    def __on_subscribe_callback(self, mqttc, obj, mid, granted_qos):
        del mqttc  # unused
        del obj  # unused
        del mid  # unused
        del granted_qos  # unused
        LOG.debug('MQTT: sucessfully subscribed to topic')

    def __on_message_callback(self, mqttc, obj, msg):  # noqa: C901
        del mqttc  # unused
        del obj  # unused
        if len(msg.payload) == 0:
            LOG.debug('MQTT: ignoring empty message')
            return

        _topic = msg.topic
        if isinstance(_topic, bytes):
            _topic = _topic.decode("utf-8")
        if not isinstance(_topic, str):
            LOG.error(f"Unable to handle topic '{_topic}', type {type(_topic)}")
            return

        _payload = msg.payload
        if isinstance(_payload, bytes):
            _payload = _payload.decode("utf-8")
        if not isinstance(_payload, str):
            LOG.error(f"Unable to handle topic '{_topic}' payload '{_payload}' , type {type(_payload)}")
            return

        _cb_func = self._subscriptions.get(_topic, None)
        if _cb_func is not None:
            try:
                _cb_func(_payload)
            except ValueError as err:
                LOG.error(f"Value set failed! {err}")
            return

        LOG.warning(f"Unknown message received: {_topic} {_payload}")

    def disconnect(self, reasoncode=None, properties=None):
        try:
            self.publish(topic=self.availability_topic, qos=1, retain=True,
                         payload="offline")  # .wait_for_publish()
        except RuntimeError:
            pass
        super().disconnect(reasoncode, properties)

    def set_connected(self, connected: bool):
        if connected != self.connected:
            self._subscriptions = {}
            for client in self._clients:
                client.connected(self)
            self.publish(topic=self.availability_topic, qos=1, retain=True,
                         payload=["offline", "online"][connected])
            self.connected = connected

    def run(self):
        self.loop_start()
        try:
            while True:
                time.sleep(self.interval)
                self.run_clients()
        except KeyboardInterrupt:
            LOG.info(" ... stopping ...")
            self.loop_stop()
        except VControldClient.ConnectionFailure:
            LOG.error("VControld: connection failure. Restart...")
            raise self.RestartRequired("vcontrold connection error")

    def run_clients(self) -> None:
        for client in self._clients:
            client.run(self)


# Code copied from weconnec-mqtt library (https://github.com/tillsteinbach/WeConnect-mqtt)
class NumberRangeArgument:

    def __init__(self, imin=None, imax=None):
        self.imin = imin
        self.imax = imax

    def __call__(self, arg):
        try:
            value = int(arg)
        except ValueError as e:
            raise self.exception() from e
        if (self.imin is not None and value < self.imin) or (self.imax is not None and value > self.imax):
            raise self.exception()
        return value

    def exception(self):
        if self.imin is not None and self.imax is not None:
            return argparse.ArgumentTypeError(f'Must be a number from {self.imin} to {self.imax}')
        if self.imin is not None:
            return argparse.ArgumentTypeError(f'Must be a number not smaller than {self.imin}')
        if self.imax is not None:
            return argparse.ArgumentTypeError(f'Must be number not larger than {self.imax}')

        return argparse.ArgumentTypeError('Must be a number')


def main():
    parser = argparse.ArgumentParser(
        prog='viessmann-optolink-mqtt',
        description='Viessmann Optolink to MQTT handler')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')

    mqtt_group = parser.add_argument_group('MQTT and Broker')
    mqtt_group.add_argument('--prefix', dest='prefix', default='homeassistant',
                            help='MQTT prefix. Default is homeassistant')
    mqtt_group.add_argument('--broker', type=str, help='MQTT broker address', required=True)
    mqtt_group.add_argument('--port', default=1883, type=NumberRangeArgument(1, 65535),
                            help='MQTT broker port', required=False)
    mqtt_group.add_argument('--user', default='', help='MQTT broker username')
    mqtt_group.add_argument('--password', default='', help='MQTT broker password')
    mqtt_group.add_argument('--clientid', default=None, help='Id of the client. Default is a random id',
                            required=False)
    mqtt_group.add_argument('--keepalive', default=60, type=int, help='Time between keep-alive messages',
                            required=False)

    optolink_group = parser.add_argument_group('Optolink')
    optolink_group.add_argument('--vcd_host', type=str, help='vcontrold telnet host address', required=True)
    optolink_group.add_argument('--vcd_port', default=3002, type=NumberRangeArgument(1, 65535),
                                help='vcontrold telnet host port', required=False)

    log_group = parser.add_argument_group('Logging')
    log_group.add_argument('-v', '--verbose', action='count', default=1,
                           help='Increase logging verbose level')

    args = parser.parse_args()

    # Configure logging
    log_level = {0: "CRITICAL", 1: "ERROR", 2: "WARNING", 3: "INFO", 4: "DEBUG"}.get(args.verbose, "DEBUG")
    logging.basicConfig(level=log_level,
                        format='%(asctime)s:%(levelname)s:%(module)s:%(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S%z')
    LOG.info('Viessmann Optolink to MQTT version %s ', __version__)

    # Create a vcontrold client
    vcd_client = VControldClient(args.vcd_host, args.vcd_port, args.prefix, LOG)
    vcd_client.version()

    mqtt_client = MqttClient(args.clientid, prefix=args.prefix)
    mqtt_client.enable_logger()
    mqtt_client.username_pw_set(args.user, args.password)
    mqtt_client.client_add(vcd_client)
    try:
        # Connect to MQTT broker
        while True:
            try:
                mqtt_client.connect(args.broker, args.port, args.keepalive)
                break
            except ConnectionRefusedError as err:
                LOG.error('Could not connect to MQTT-Server: %s, will retry in 10 seconds', err)
                time.sleep(10)
        mqtt_client.run()
        mqtt_client.disconnect()
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
