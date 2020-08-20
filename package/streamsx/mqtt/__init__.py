# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020

"""
Overview
++++++++

Provides functions:

* to connect to an MQTT server
* to subscribe to topics and receive events
* to publish events


Sample
++++++

A simple example of a Streams application uses the :py:class:`~MQTTSink` and :py:class:`~MQTTSource` classes::

    from streamsx.mqtt import MQTTSource, MQTTSink
    from streamsx.topology import context
    from streamsx.topology.topology import Topology
    from streamsx.topology.schema import CommonSchema
    
    mqtt_server_uri = 'tcp://host.domain:1883'
    s = 'Each character will be an MQTT message'
    topology = Topology()
    data = topology.source([c for c in s]).as_string()
    # publish to MQTT
    data.for_each(MQTTSink(server_uri=mqtt_server_uri, topic='topic'),
                  name='MQTTpublish')
    
    # subscribe for data and print to stdout
    received = topology.source(MQTTSource(mqtt_server_uri, schema=CommonSchema.String, topics='topic'))
    received.print()
    
    context.submit(context.ContextTypes.DISTRIBUTED, topology)
    # the Streams Job keeps running and must be cancelled manually

"""


__version__='1.0.1'

__all__ = ['MQTTSink', 'MQTTSource']
from streamsx.mqtt._mqtt import MQTTSink, MQTTSource

