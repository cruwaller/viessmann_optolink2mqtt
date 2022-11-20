import sys
import time
from datetime import timedelta, datetime
import telnetlib
import re
import argparse
import logging
import json
import socket
import paho.mqtt.client as mqtt

from __version import __version__
from homeassistant_auto_discovery import (
    HassDevice,
    HassSensor, HassBinarySensor, HassSelect,
)

# 192.168.1.250:30080


LOG = logging.getLogger("optolink2mqtt")

DEVICE = HassDevice("vcontrold", "Viessmann Optolink",
                    __version__, "unknown", "Viessmann")


class VitoConnection(object):
    def __init__(self, telnet: telnetlib.Telnet, cmd: str) -> None:
        self.telnet_client = telnet
        self.cmd = cmd

    def set_telnet_client(self, telnet):
        self.telnet_client = telnet

    def read(self, until: str) -> str:
        if isinstance(until, str):
            until = str.encode(until)
        _resp = self.telnet_client.read_until(until, timeout=10).decode().strip()
        return _resp.replace("vctrld>", "")  # remove possible vcontrold cli "header"

    def read_line(self) -> str:
        return self.read('\n')

    def send(self, cmd: str = None) -> None:
        if not isinstance(cmd, str):
            raise ValueError("command must be string")
        self.telnet_client.write(str.encode(cmd + '\n'))

    def query(self) -> str:
        if not self.cmd:
            return
        # self.read("vctrld>")
        self.telnet_client.read_very_eager()
        self.send(self.cmd)
        resp = self.read_line()
        return resp


class VitoElementBinary(HassBinarySensor):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd)
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        val = self.conn.query()
        return ["ON", "OFF"][val in ["0", "Off"]]

    @value.setter
    def value(self, value):
        raise ValueError("Does not support set value!")

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
        self.error_count +=1
        return None

    @value.setter
    def value(self, value):
        raise ValueError("Does not support set value!")

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
        self.error_count +=1
        return None

    @value.setter
    def value(self, value):
        raise ValueError("Does not support set value!")

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
        self.error_count +=1
        return None

    @value.setter
    def value(self, value):
        raise ValueError("Does not support set value!")

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementSelect(HassSelect):
    def __init__(self, name: str, cmd: str, telnet: telnetlib.Telnet,
                 options: dict = {}, write: bool = False) -> None:
        super().__init__(name, DEVICE, topic_parent_level=cmd,
                         category="config", options=list(options.keys()), cmd=True)
        self.conn = VitoConnection(telnet, cmd)
        self.options: dict = options

    @property
    def value(self) -> None:
        val = self.conn.query()
        for key, value in self.options.items():
            if value == val:
                return key
        self.error_count +=1
        return ""

    @value.setter
    def value(self, val):
        for key, value in self.options.items():
            if key == val:
                LOG.info(f"{self.name}: new value {value}")
                # TODO: implement send to pump!

    def value_with_topic(self):
        return (self.get_topic(), self.value)


class VitoElementText():
    def __init__(self, cmd: str, telnet: telnetlib.Telnet) -> None:
        self.conn = VitoConnection(telnet, cmd)

    @property
    def value(self) -> None:
        return self.conn.query()

    @value.setter
    def value(self, value):
        raise ValueError("Does not support set value!")


class VitoElementTextWrite(VitoElementText):
    @VitoElementText.value.setter
    def value(self, value) -> None:
        if not isinstance(value, str):
            raise ValueError("Only string is supported")
        # TODO: write text...


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
            VitoElementTemperature("Room temperature, normal", "RoomTempNormal", telnet),
            VitoElementTemperature("Heating", "Heating", telnet),
            VitoElementTemperature("Circuit temperature, primary, flow", "PrimaryCircuitFlowTemp", telnet),
            VitoElementTemperature("Circuit temperature, primary, return", "PrimaryCircuitReturnTemp", telnet),
            VitoElementTemperature("Circuit temperature, secondary, flow", "SecondaryCircuitFlowTemp", telnet),
            VitoElementTemperature("Circuit temperature, secondary, return", "SecondaryCircuitReturnTemp", telnet),
            VitoElementTemperature("DHW Setpoint Normal", "DHW-Setpoint", telnet),
            VitoElementTemperature("DHW Setpoint High", "DHW-Setpoint2", telnet),
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
                              }, True),
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
        for sensor in self.sensors:
            config = sensor.get_config(availability_topic)
            mqtt.publish(*config, retain=True)
            # self.logger.debug(f"Publish: {config}")

    def run(self, mqtt) -> list:
        try:
            self.get_device_type()  # to make sure the connection is ok
        except self.ConnectionFailure:
            self.__connect_telnet_client()
            return  # Will reschedule soon...
        for sensor in self.sensors:
            update = sensor.value_with_topic()
            mqtt.publish(*update, retain=True)
            # self.logger.debug(f"Publish: {update}")


class MqttClient(mqtt.Client):
    class RestartRequired(Exception):
        pass

    def __init__(self, client_id=None, transport="tcp", reconnect_on_failure=True,
                 prefix='homeassistant', interval=30):
        super().__init__(client_id=client_id, transport=transport,
                         reconnect_on_failure=reconnect_on_failure)
        self._clients: ClientBase = []

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

    def __on_connect_callback(self, mqttc, obj, flags, rc):
        del mqttc  # unused
        del obj  # unused
        del flags  # unused

        if rc == 0:
            LOG.info('Connected to MQTT broker')
            # Subscribe for topis
            topic = f'{self.cfg_topic_prefix}/forcedUpdate_write'
            self.subscribe(topic, qos=2)

            topic = f'{self.cfg_topic_prefix}/updateInterval_s'
            self.publish(topic=topic, qos=1, retain=True,
                            payload=self.interval)
            self.subscribe(topic + '_write', qos=1)
            self.set_connected(True)
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
        elif msg.topic == f'{self.cfg_topic_prefix}/forcedUpdate_write':
            if msg.payload.lower() == b'True'.lower():
                LOG.info('MQTT received: Forced update triggered')
                self.run_clients()
        elif msg.topic == f'{self.cfg_topic_prefix}/updateInterval_s_write':
            if str(msg.payload.decode()).isnumeric():
                newInterval = int(msg.payload)
                # TODO: range check...MQTT-Server
                self.interval = newInterval
                LOG.info('MQTT received: Interval updated: %ds' % newInterval)
            else:
                LOG.error(f'MQTT received: Interval is not a number: {msg.payload.decode()}')
            # publish new/old value
            self.publish(topic=f'{self.cfg_topic_prefix}/updateInterval_s', qos=1, retain=True,
                         payload=self.interval)
        else:
            if msg.topic.startswith(self.prefix):
                address = msg.topic[len(self.prefix):]
                if address.endswith('_write'):
                    address = address[:-len('_write')]
                    LOG.info(f"MQTT received. Address '{address}'")
                    # TODO handle write commands

    def disconnect(self, reasoncode=None, properties=None):
        try:
            self.publish(topic=self.availability_topic, qos=1, retain=True,
                         payload="offline") #.wait_for_publish()
        except RuntimeError:
            pass
        super().disconnect(reasoncode, properties)

    def set_connected(self, connected: bool):
        if connected != self.connected:
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
            self.loop_stop(force=False)
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
    mqtt_group.add_argument('--vcd_host', type=str, help='vcontrold telnet host address', required=True)
    mqtt_group.add_argument('--vcd_port', default=3002, type=NumberRangeArgument(1, 65535),
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
