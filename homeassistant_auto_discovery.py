import json


class HassDevice(dict):
    def __init__(self, identifiers, name: str, sw_version: str, model: str, manufacturer: str,
                 discovery_prefix: str = "homeassistant"):
        super().__init__()
        self.discovery_prefix = discovery_prefix
        self.prefix = ""
        if isinstance(identifiers, str):
            identifiers = [identifiers]
        elif not isinstance(identifiers, list):
            raise ValueError("identifiers must be a list!")
        self.name = name.replace(" ", "_").lower()
        self["identifiers"] = identifiers
        self["name"] = name
        self["sw_version"] = sw_version
        self["model"] = model
        self["manufacturer"] = manufacturer

    def set_model(self, model):
        if model:
            self["model"] = model

    def set_prefix(self, prefix):
        self.prefix = f"{prefix}/" if prefix else ""


class HassComponent:
    def __init__(self, component: str, name: str, device: HassDevice,
                 device_class: str, unit_of_measurement: str, icon: str,
                 force_update: bool, topic_parent_level: str, state_class: str,
                 category: str, options: list = None, cmd: bool = False):
        self.topic_parent_level = topic_parent_level
        self.discovery_prefix = device.discovery_prefix
        self.component = component  # sensor, binary_sensor, switch...
        self.name = name
        self.unique_id = (device.name + name).replace(",", "").replace(" ", "_").lower()
        topic = f"{device.prefix}{device.name}/{self.component}/{self.get_parent()}{self.unique_id}"
        config = {
            "name": name,
            "device": device,
            "enabled_by_default": True,
            "unique_id": self.unique_id,
            "force_update": force_update,
            "platform": "mqtt",
            "~": topic,
            "stat_t": "~/state",  # state_topic

            # **** JSON version ****
            # "json_attributes_topic": "",
            # "value_template": "{{ value_json.battery }}",
        }
        if cmd:
            config["cmd_t"] = "~/set"  # command_topic
        if unit_of_measurement:
            config["unit_of_measurement"] = unit_of_measurement
        if device_class:
            config["device_class"] = device_class
        if state_class:
            config["state_class"] = state_class
        if icon:
            config["icon"] = icon
        if options:
            config["options"] = options
        self.config = config
        self.topic = topic
        self.set_category(category)
        self.error_count = 0

    def set_category(self, category):
        if category:
            self.config["entity_category"] = category

    def get_parent(self):
        if self.topic_parent_level:
            return f"{self.topic_parent_level}/"
        return ""

    def get_config(self, availability_topic=""):
        if availability_topic:
            self.config["availability"] = [{"topic": availability_topic}]
        topic = f"{self.discovery_prefix}/{self.component}/{self.unique_id}/config"
        return (topic, json.dumps(self.config))

    def get_topic(self) -> str:
        return f"{self.topic}/state"

    def get_topic_set(self) -> str:
        return f"{self.topic}/set" if "cmd_t" in self.config else ""


class HassSensor(HassComponent):
    def __init__(self, name, parent_device, unit_of_measurement=None,
                 icon=None, topic_parent_level="", force_update=False,
                 device_class = None, state_class=None, category=None):
        super().__init__("sensor", name, parent_device, device_class,
                         unit_of_measurement, icon, force_update,
                         topic_parent_level, state_class, category)


class HassBinarySensor(HassComponent):
    def __init__(self, name, parent_device,
                 icon=None, topic_parent_level="", force_update=False,
                 device_class=None, category=None):
        super().__init__("binary_sensor", name, parent_device, device_class,
                         None, icon, force_update,
                         topic_parent_level, None, category)


class HassSelect(HassComponent):
    def __init__(self, name, parent_device, options,
                 icon=None, topic_parent_level="", force_update=False,
                 category=None, cmd=False):
        super().__init__("select", name, parent_device, None,
                         None, icon, force_update,
                         topic_parent_level, None, category,
                         options=options, cmd=cmd)
