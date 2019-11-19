import streamsx.mqtt as mqtt

import typing
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr
import unittest
import datetime
import os
import pathlib
import json
from subprocess import call, Popen, PIPE

def cloud_creds_env_var():
    result = True
    try:
        os.environ['IOT_SERVICE_CREDENTIALS']
    except KeyError: 
        result = False

    return result

class MqttDataTuple(typing.NamedTuple):
    topic_name: str
    data:       str


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))
        self.mqtt_toolkit_home = os.environ["MQTT_TOOLKIT_HOME"]
        
    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    def _get_app_config(self):
        creds_file = pathlib.Path.cwd().joinpath('streamsx','mqtt', 'tests', 'appConfig.json')
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        
        # credentials = json.loads('{"userID" : "user", "password" : "xxx", "serverURI" : "xxx" }')
        return credentials

    def _get_device_config(self):
        creds_file = pathlib.Path.cwd().joinpath('streamsx','mqtt', 'tests', 'deviceConfig.json')
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        return credentials

    def _create_stream(self, topo):
        s = topo.source([("test",'{"id":"testid1"}'),("test",'{"id":"testid2"}'),("test",'{"id":"testid3"}')])
        return s.map(lambda x : {'topic_name':x[0],'data':x[1]}, schema=MqttDataTuple)

    def test_bad_param(self):
        print ('\n---------'+str(self))
        name = 'test_bad_param'
        topo = Topology(name)
        config = {}
        # mqtt_source : expect ValueError, no Topic (parameter is None)
        self.assertRaises(ValueError, mqtt.mqtt_source, topo,MqttDataTuple,{},None,topic_attribute_name = 'topic_name' )
        # mqtt_sink : expect ValueError, topic and topic_attribute_name is set
        test_stream = self._create_stream(topo)
        self.assertRaises(ValueError, mqtt.mqtt_sink,test_stream,{},topic='test_topic',topic_attribute_name='topic_name')
        # mqtt_sink : expect ValueError, topic and topic_attribute_name are both not set
        self.assertRaises(ValueError, mqtt.mqtt_sink,test_stream,{})
        # expect ValueError 
        # self.assertRaises(TypeError, mqtt.mqtt_source, topo, )

    def test_mqqt_source(self):
        print ('\n---------'+str(self))
        name = 'test_mqtt_source'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.mqtt_toolkit_home)
        mqtt_config = self._get_app_config()
        source_stream = mqtt.mqtt_source(topo, [MqttDataTuple], mqtt_config, 'test_topic', topic_attribute_name = 'topic_name')
        source_stream.print()
        # build only
        self._build_only(name, topo)


    def test_mqqt_sink(self):
        print ('\n---------'+str(self))
        name = 'test_mqtt_sink'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.mqtt_toolkit_home)
        mqtt_config = self._get_app_config()
        test_stream = self._create_stream(topo)
        test_stream.print()
        mqtt.mqtt_sink(test_stream,mqtt_config,topic='test_topic')

        source_stream = mqtt.mqtt_source(topo, [MqttDataTuple], mqtt_config, 'test_topic', topic_attribute_name = 'topic_name')
        source_stream.print()

        # build only
        self._build_only(name, topo)


    # test to connect to an IBM Cloud IOT Service which is in it's base
    # a MQTT broker
    # The IOTF Service needs to have a DeviceType 'Test' and an IOT device of this DeviceType
    # Two credential files have to be created from the templates in teswt directory
    # deviceCredentials.json and appCredentials.json with the appropriate values
    # matching the used IBM Cloud IOT service and device
    # WHen device is created it's credentials have to be noted.
    # An application key has to be created for a 'Standard Application', they have also
    # be noted on creation time as the secrets are not shown later.
    def test_device_app(self):
        print ('\n---------'+str(self))
        name = 'test_device_app'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.mqtt_toolkit_home)

        # generate IOT specific Topic for Application
        device_type = "Test"
        event_id = "data"
        device_id = "+"
        message_format = "json"
        # app subscribe topic "iot-2/type/device_type/id/device_id/evt/event_id/fmt/format_string"
        app_subscribe_topic = "iot-2/type/"+device_type+"/id/"+device_id+"/evt/"+event_id+"/fmt/"+message_format
        #mqtt config is dict as read by from JSON, JSON uses already correct parameter values
        app_config = self._get_app_config()
        source_stream = mqtt.mqtt_source(topo, [MqttDataTuple], app_config, app_subscribe_topic, topic_attribute_name = 'topic_name')
        source_stream.print()


        test_stream = self._create_stream(topo)
        test_stream.print()
        # generate IOT specific Topic for device
        # event_id has to be same as for application 
        # message_format has to be same as for application 
        # device event publish topic = "iot-2/evt/event_id/fmt/format_string"
        device_topic = "iot-2/evt/"+event_id+"/fmt/" + message_format
        # device config is dict as read by from JSON, JSON uses already correct parameter values
        device_config = self._get_device_config()
        mqtt.mqtt_sink(test_stream,device_config,topic=device_topic)

        if (("TestDistributed" in str(self)) or ("TestStreamingAnalytics" in str(self))):
            self._launch(topo)
        else:
            # build only
            self._build_only(name, topo)




class TestDistributed(Test):
    def setUp(self):
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False  


    def _launch(self, topo):
        rc = streamsx.topology.context.submit('DISTRIBUTED', topo, self.test_config)
        print(str(rc))
        if rc is not None:
            if (rc.return_code == 0):
                rc.job.cancel()

class TestStreamingAnalytics(Test):
    def setUp(self):
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)

    def _launch(self, topo):
        rc = streamsx.topology.context.submit('STREAMING_ANALYTICS_SERVICE', topo, self.test_config)
        print(str(rc))
        if rc is not None:
            if (rc.return_code == 0):
                rc.job.cancel()

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()
        super().setUpClass()

