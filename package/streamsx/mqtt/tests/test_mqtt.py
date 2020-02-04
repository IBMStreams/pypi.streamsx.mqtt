from streamsx.mqtt import MQTTSource, MQTTSink

import typing
from streamsx.topology.topology import Topology
from streamsx.topology.context import ContextTypes
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.mqtt.tests.x509_certs import TRUSTED_CERT_PEM, PRIVATE_KEY_PEM, CLIENT_CERT_PEM, CLIENT_CA_CERT_PEM
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
    data: str


class TestParams(unittest.TestCase):
    def test_bad_param(self):
        print ('\n---------'+str(self))
        # constructor tests
        self.assertRaises(ValueError, MQTTSource, server_uri=None, topics='topic1', schema=MqttDataTuple)
        # topics = None
        self.assertRaises(ValueError, MQTTSource, server_uri='tcp://server:1234', topics=None, schema=MqttDataTuple)
        # schema = None
        self.assertRaises(ValueError, MQTTSource, server_uri='tcp://server:1234', topics='topic1', schema=None)
        # server_uri = None
        self.assertRaises(ValueError, MQTTSink, server_uri=None, topic='topic1')
        # topic and topic_attribute_name = None
        self.assertRaises(ValueError, MQTTSink, server_uri='tcp://server:1234', topic=None, topic_attribute_name=None)
        # topic and topic_attribute_name = Not None
        self.assertRaises(ValueError, MQTTSink, server_uri='tcp://server:1234', topic='topic1', topic_attribute_name='topic')
   
        # setter tests
        src = MQTTSource(server_uri='tcp://server:1833', topics=['topic1', 'topic2'], schema=MqttDataTuple)
        with self.assertRaises(TypeError):
            src.qos = ['0', '2']
        with self.assertRaises(ValueError):
            src.qos = 3
        with self.assertRaises(ValueError):
            src.qos = -1
        with self.assertRaises(ValueError):
            src.qos = [0, 3]
        with self.assertRaises(ValueError):
            src.reconnection_bound = -2
        with self.assertRaises(ValueError):
            src.keep_alive_seconds = -1
        with self.assertRaises(ValueError):
            src.command_timeout_millis = -1
        with self.assertRaises(ValueError):
            src.message_queue_size = 0
            
        sink = MQTTSink(server_uri='tcp://server:1833', topic='topic1')
        with self.assertRaises(ValueError):
            sink.qos = 3
        with self.assertRaises(ValueError):
            sink.qos = -1
        with self.assertRaises(TypeError):
            sink.qos = [0, 3]
        with self.assertRaises(ValueError):
            sink.reconnection_bound = -2
        with self.assertRaises(ValueError):
            sink.keep_alive_seconds = -1
        with self.assertRaises(ValueError):
            sink.command_timeout_millis = -1

    def test_options_kwargs_MQTTSink(self):
        print ('\n---------'+str(self))
        sink = MQTTSink(server_uri='tcp://server:1833',
                        topic='topic1',
                        data_attribute_name='data',
                        #kwargs
                        vm_arg = ["-Xmx1G"],
                        ssl_debug = True,
                        reconnection_bound = 5,
                        qos = 2,
                        trusted_certs = ['cert1', 'cert2'],
                        truststore = "/truststore",
                        truststore_password = "trustpasswd",
                        client_cert = 'client_cert',
                        client_private_key = 'private_key',
                        keystore = "/keystore",
                        keystore_password = "keypasswd",
                        ssl_protocol = 'TLSv1.2',
                        app_config_name = "abbconf",
                        client_id = "client-IDsink",
                        command_timeout_millis = 47,
                        keep_alive_seconds = 3,
                        password = "passw0rd",
                        username = "rolef",
                        retain = True)
        self.assertEqual(sink.server_uri, 'tcp://server:1833')
        self.assertEqual(sink._topic, 'topic1')
        self.assertEqual(sink._data_attribute_name, 'data')
        self.assertEqual(sink.reconnection_bound, 5)
        self.assertEqual(sink.ssl_debug, True)
        self.assertListEqual(sink.vm_arg, ["-Xmx1G"])
        self.assertEqual(sink.qos, 2)
        self.assertListEqual(sink.trusted_certs, ['cert1', 'cert2'])
        self.assertEqual(sink.truststore, '/truststore')
        self.assertEqual(sink.truststore_password, 'trustpasswd')
        self.assertEqual(sink.client_cert, 'client_cert')
        self.assertEqual(sink.client_private_key, 'private_key')
        self.assertEqual(sink.keystore, '/keystore')
        self.assertEqual(sink.keystore_password, 'keypasswd')
        self.assertEqual(sink.ssl_protocol, 'TLSv1.2')
        self.assertEqual(sink.app_config_name, 'abbconf')
        self.assertEqual(sink.client_id, 'client-IDsink')
        self.assertEqual(sink.command_timeout_millis, 47)
        self.assertEqual(sink.keep_alive_seconds, 3)
        self.assertEqual(sink.password, 'passw0rd')
        self.assertEqual(sink.username, 'rolef')
        self.assertEqual(sink.retain, True)

    def test_options_kwargs_MQTTSource(self):
        print ('\n---------'+str(self))
        src = MQTTSource(server_uri='tcp://server:1833',
                         topics=['topic1', 'topic2'],
                         schema=[MqttDataTuple],
                         data_attribute_name='data',
                         #kwargs
                         vm_arg = ["-Xmx1G"],
                         ssl_debug = True,
                         reconnection_bound = 5,
                         qos = [1, 2],
                         message_queue_size = 122,
                         trusted_certs = ['cert1', 'cert2'],
                         truststore = "/truststore",
                         truststore_password = "trustpasswd",
                         client_cert = 'client_cert',
                         client_private_key = 'private_key',
                         keystore = "/keystore",
                         keystore_password = "keypasswd",
                         ssl_protocol = 'TLSv1.2',
                         app_config_name = "abbconf",
                         client_id = "client-IDsink",
                         command_timeout_millis = 47,
                         keep_alive_seconds = 3,
                         password = "passw0rd",
                         username = "rolef")
        self.assertEqual(src.server_uri, 'tcp://server:1833')
        self.assertListEqual(src._topics, ['topic1', 'topic2'])
        self.assertEqual(src._data_attribute_name, 'data')
        self.assertEqual(src.reconnection_bound, 5)
        self.assertEqual(src.ssl_debug, True)
        self.assertListEqual(src.vm_arg, ["-Xmx1G"])
        self.assertListEqual(src.qos, [1,2])
        self.assertListEqual(src.trusted_certs, ['cert1', 'cert2'])
        self.assertEqual(src.truststore, '/truststore')
        self.assertEqual(src.truststore_password, 'trustpasswd')
        self.assertEqual(src.client_cert, 'client_cert')
        self.assertEqual(src.client_private_key, 'private_key')
        self.assertEqual(src.keystore, '/keystore')
        self.assertEqual(src.keystore_password, 'keypasswd')
        self.assertEqual(src.ssl_protocol, 'TLSv1.2')
        self.assertEqual(src.app_config_name, 'abbconf')
        self.assertEqual(src.client_id, 'client-IDsink')
        self.assertEqual(src.command_timeout_millis, 47)
        self.assertEqual(src.keep_alive_seconds, 3)
        self.assertEqual(src.password, 'passw0rd')
        self.assertEqual(src.username, 'rolef')
        self.assertEqual(src.message_queue_size, 122)
    
    
class Test(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print (str(self))
        self.mqtt_toolkit_home = os.environ["MQTT_TOOLKIT_HOME"]
        
    def _build_only(self, name, topo):
        result = streamsx.topology.context.submit(ContextTypes.TOOLKIT, topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit(ContextTypes.BUNDLE, topo.graph)  # creates sab file
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


    def test_compile_MQTTSource(self):
        print ('\n---------'+str(self))
        name = 'test_MQTTSource'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.mqtt_toolkit_home)
        src = MQTTSource(server_uri='tcp://server:1833', topics=['topic1', 'topic2'], schema=[MqttDataTuple])
        # simply add all parameters; let' see if it compiles
        src.qos = [1, 2]
        src.message_queue_size = 122
        src.client_id = "client-IDsrc"
        src.reconnection_bound = 25
        src.trusted_certs = [TRUSTED_CERT_PEM, CLIENT_CA_CERT_PEM]
        src.client_cert = CLIENT_CERT_PEM
        src.client_private_key = PRIVATE_KEY_PEM
        src.ssl_protocol = 'TLSv1.1'
        src.vm_arg = ["-Xmx13G"]
        src.ssl_debug = True
        src.app_config_name = "abbconf2"
        src.command_timeout_millis=30000
        src.keep_alive_seconds = 65
        src.password = "passw0rd2"
        src.username = "it_is_me"
        src.app_config_name = "mqtt_app_cfg"
        
        source_stream = topo.source(src, name='MqttStream')
        source_stream.print()
        # build only
        self._build_only(name, topo)

    def test_compile_MQTTSink(self):
        print ('\n---------'+str(self))
        name = 'test_MQTTSink'
        topo = Topology(name)
        streamsx.spl.toolkit.add_toolkit(topo, self.mqtt_toolkit_home)
        test_stream = self._create_stream(topo)
        sink = MQTTSink(server_uri='tcp://server:1833', topic='topic1', data_attribute_name='data')
        # simply add all parameters; let' see if it compiles
        sink.reconnection_bound=5
        sink.qos=2
        sink.trusted_certs = [TRUSTED_CERT_PEM, CLIENT_CA_CERT_PEM]
        sink.client_cert = CLIENT_CERT_PEM
        sink.client_private_key = PRIVATE_KEY_PEM
        sink.ssl_protocol = 'TLSv1.2'
        sink.vm_arg = ["-Xmx1G"]
        sink.ssl_debug = True
        sink.app_config_name = "abbconf"
        sink.client_id = "client-IDsink"
        sink.command_timeout_millis=47
        sink.keep_alive_seconds = 3
        sink.password = "passw0rd"
        sink.username = "rolef"
        sink.retain = True

        test_stream.for_each(sink, name='MQTTPublish')
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
        mqtt_source = MQTTSource(server_uri=app_config['serverURI'], topics=app_subscribe_topic, schema=[MqttDataTuple], topic_attribute_name='topic_name')
        mqtt_source.username = app_config['userID']
        mqtt_source.password = app_config['password']
        mqtt_source.client_id = app_config['clientID']
        mqtt_source.vm_arg = "-Dcom.ibm.jsse2.overrideDefaultTLS=true"
        source_stream = topo.source(mqtt_source, name='MqttSubscribe')
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
        mqtt_sink = MQTTSink(server_uri=device_config['serverURI'], topic=device_topic, data_attribute_name='data')
        mqtt_sink.client_id = device_config['clientID']
        mqtt_sink.username = device_config['userID']
        mqtt_sink.password = device_config['password']
        mqtt_sink.vm_arg = "-Dcom.ibm.jsse2.overrideDefaultTLS=true"

        test_stream.for_each(mqtt_sink, name='MqttPublish')

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
                pass
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

