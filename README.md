# Preconditions

Make sure you have python3.8 or newer installed.

# Install

- clone repository to /opt
- install requirements `pip3 install -r requirements.txt`
- set mqtt broker and vcontrold infos to service file
- copy a service file `viessmann2mqtt.service` to /etc/systemd/system/
- enable the service `systemctl enable viessmann2mqtt.service`
- start the service `systemctl start viessmann2mqtt.service`

enjoy :)
