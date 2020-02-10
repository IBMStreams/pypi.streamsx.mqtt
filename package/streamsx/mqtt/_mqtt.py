# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2020

import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.composite import Source as AbstractSource
from streamsx.topology.composite import ForEach as AbstractSink
from streamsx.topology.schema import CommonSchema
from streamsx.toolkits import create_keystore, create_truststore, extend_keystore, extend_truststore
from tempfile import gettempdir
import string
import random
import os

_TOOLKIT_NAME = 'com.ibm.streamsx.mqtt'

def _generate_random_digits(len=10):
    """
    Generate a string of random digits, default lengh is 10
    """
    return ''.join(random.choice(string.digits) for _ in range(len))


class MQTTComposite(object):
    _APP_CONFIG_PROP_NAME_FOR_PASSWORD = 'password'
    _APP_CONFIG_PROP_NAME_FOR_USERNAME = 'username'

    def __init__(self, **options):
        self._vm_arg = None
        self._app_config_name = None
        self._username = None
        self._password = None
        self._trusted_certs = None
        self._truststore = None
        self._truststore_password = None
        self._client_cert = None
        self._client_private_key = None
        self._keystore = None
        self._keystore_password = None
        self._ssl_protocol = None
        self._server_uri = None
        self._reconnection_bound = -1
        self._keep_alive_seconds = 60
        self._command_timeout_millis = None
        self._client_id = None
        self._ssl_debug = False
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')
        if 'ssl_debug' in options:
            self.ssl_debug = options.get('ssl_debug')
        if 'app_config_name' in options:
            self.app_config_name = options.get('app_config_name')
        if 'username' in options:
            self.username = options.get('username')
        if 'password' in options:
            self.password = options.get('password')
        if 'trusted_certs' in options:
            self.trusted_certs = options.get('trusted_certs')
        if 'truststore' in options:
            self.truststore = options.get('truststore')
        if 'truststore_password' in options:
            self.truststore_password = options.get('truststore_password')
        if 'client_cert' in options:
            self.client_cert = options.get('client_cert')
        if 'client_private_key' in options:
            self.client_private_key = options.get('client_private_key')
        if 'keystore' in options:
            self.keystore = options.get('keystore')
        if 'keystore_password' in options:
            self.keystore_password = options.get('keystore_password')
        if 'ssl_protocol' in options:
            self.ssl_protocol = options.get('ssl_protocol')
        if 'reconnection_bound' in options:
            self.reconnection_bound = options.get('reconnection_bound')
        if 'keep_alive_seconds' in options:
            self.keep_alive_seconds = options.get('keep_alive_seconds')
        if 'command_timeout_millis' in options:
            self.command_timeout_millis = options.get('command_timeout_millis')
        if 'client_id' in options:
            self.client_id = options.get('client_id')

    @property
    def ssl_debug(self):
        """
        bool: When ``True`` the property enables verbose SSL debug output at runtime.
        """
        return self._ssl_debug
    
    @ssl_debug.setter
    def ssl_debug(self, ssl_debug:bool):
        self._ssl_debug = ssl_debug

    @property
    def vm_arg(self):
        """
        str|list: Arguments for the Java Virtual Machine used at Runtime, for example ``-Xmx2G``.
             For multiple arguments, use a list::
             
                 mqtt.vm_arg = ["-Xmx2G", "-Xms1G"]
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, vm_arg):
        if vm_arg:
            self._vm_arg = vm_arg

    @property
    def app_config_name(self):
        """
        str: The name of an application configuration with username and password for login to the MQTT server.
        The application configuration must contain the properties
        ``username`` and ``password`` and exist when the topology is submitted. Username and password given in
        an application configuration override :py:attr:`password` and :py:attr:`username`.
        """
        return self._app_config_name

    @app_config_name.setter
    def app_config_name(self, app_config_name: str):
        self._app_config_name = app_config_name

    @property
    def username(self):
        """
        str: The username for login to the MQTT server. This property is overridden by the username given in an application configuration.
        """
        return self._username

    @username.setter
    def username(self, username: str):
        self._username = username

    @property
    def password(self):
        """
        str: The password for login to the MQTT server. This property is overridden by the password given in an application configuration.
        """
        return self._password

    @password.setter
    def password(self, password: str):
        self._password = password

    @property
    def trusted_certs(self):
        """
        str|list: Certificates or filenames with certificates in PEM format that shall be trusted when SSL connections are made.
        Self-signed server certificates or the root certificate of server certificates must go in here.
        When no truststore is given with :py:attr:`truststore`, a new truststore is created and added as a file dependency
        to the topology. Otherwise the certificates are added to the given truststore, and the truststore is added as
        a file dependency.
        
        Example with one certificate literally::
        
            mqtt_comp = ...
            mqtt_comp.trusted_certs = \"\"\"
            -----BEGIN CERTIFICATE-----
            MIIDJzCCAg+gAwIBAgIJAJhu1pO2qbj7MA0GCSqGSIb3DQEBCwUAMCoxEzARBgNV
            BAoMCmlvLnN0cmltemkxEzARBgNVBAMMCmNsdXN0ZXItY2EwHhcNMTkwODE2MDc1
            YSTwYvxfkrxGKtDiLjIZ6Q6LJjMcWLG4x3I0WmGgvytTs04S4B+1vp721jmqRKm9
            ...
            ocAT5iL3ZDUj/lwqJRptmzGFcdko+woFae68HRx1ygSgROls7bXy/CwgME0LFFQp
            B+2YAhUw1sPU410JUxU3/v6R5vJfI9imE75aha3U7UbeOX8+1+Cu3HOT1QMn80k2
            6LnZeMCCgCBp+Yz3YNeUMRejMU6x4WlhTPO7bBq3tKGgwCoyGIX25wMM1Q==
            -----END CERTIFICATE-----
            \"\"\"

        Example with two certificates stored in files in */tmp*::

            mqtt_comp = ...
            mqtt_comp.trusted_certs = ['/tmp/ca_cert_1.pem', '/tmp/ca_cert_2.pem']

        """
        if self._trusted_certs:
            if len(self._trusted_certs) == 1 and self._certs_as_str:
                return self._trusted_certs[0]
        return self._trusted_certs

    @trusted_certs.setter
    def trusted_certs(self, trusted_certs):
        if isinstance(trusted_certs, str):
            c = trusted_certs.strip()
            if c:
                self._trusted_certs = [c]
                self._certs_as_str = True
        elif isinstance(trusted_certs, list):
            if trusted_certs:
                # empty list and empty str evaluate to FALSE
                self._trusted_certs = trusted_certs
                self._certs_as_str = False
        else:
            raise TypeError(trusted_certs)

    @property
    def truststore(self):
        """
        str: The filename of a truststore with certificates that shall be trusted when SSL connections are made.
        This truststore file is added as a file dependency to the topology.
        
        .. note::
            When this property is set, also the :py:attr:`truststore_password` must be given.
            
        """
        return self._truststore

    @truststore.setter
    def truststore(self, truststore: str):
        self._truststore = truststore

    @property
    def truststore_password(self):
        """
        str: The password of the truststore given in :py:attr:`truststore`.
        """
        return self._truststore_password

    @truststore_password.setter
    def truststore_password(self, truststore_password: str):
        self._truststore_password = truststore_password

    @property
    def client_cert(self):
        """
        str: A client certificate or a filename with a client certificate.
        Certificates must be given in PEM format or as a filename that contains the PEM formatted certificate.
        
        When no keystore file is given with :py:attr:`keystore`, a new keystore is created and added as a file dependency
        to the topology. Otherwise the certificate is added to the given keystore, 
        and the keystore is added as a file dependency.

        The private key of the certificate must be provided in :py:attr:`client_private_key`.
        """
        return self._client_cert

    @client_cert.setter
    def client_cert(self, client_cert: str):
        self._client_cert = client_cert

    @property
    def client_private_key(self):
        """
        str: The private RSA key of a client certificate or a filename with the private key.
        The key must be given in PEM format literally or as a filename.
        The client certificate must be provided in :py:attr:`client_cert`.
        
        Example::
        
            private_key = \"\"\"
            -----BEGIN PRIVATE KEY-----
            MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDBqvzZWSC+cF0T
            cTk6xC9iJDdWyqamh/9HnOVWDLzAKQ0XsdSHONVLjs3fwY8bM1tZywBDUujCujJT
            NaAW3YDkiVG8v+j90eR6UUc/zlG5zC6tZ6/UlHgXSUOCVuB7d3Y3v9Aoiz8HfPEi
            0uV+6WOHCPg9df0gwmzPRYDtobMWat0jCXLkekLjc2gx8muPsOnMAaepNHYmfvFn
            ...
            I4ahg9LgF3pT8BEr5xxfwX3U/i0pHz5qc+kuuXxfAoGBAI0SmUDSGT7rTgxvwy7o
            fRIOJz3KlIYZ+JugC6Zj7GYv7fISzVRVENpaeL6jPGmhId/Pu++yT7x9FdmlGfMv
            UGu65oHRMYGb+dMCeYF6XymSXy1MOHTH38AE27/BHm8bDe/CYJgcKXo+3FvCRal1
            YBdL1LnNMCOKPo2gPB19HgR7
            -----END PRIVATE KEY-----
            \"\"\"
            
            mqtt = ...
            mqtt.client_private_key = private_key
        
        Key is stored in a file::
        
            mqtt = ...
            mqtt.client_private_key = '/home/user/private_key.pem'
        
        """
        return self._client_private_key

    @client_private_key.setter
    def client_private_key(self, client_private_key: str):
        self._client_private_key = client_private_key

    @property
    def keystore(self):
        """
        The filename of a keystore with client certificate and its private key.
        This keystore file is added as a file dependency to the topology.
        
        .. note::
            When this property is set, also the :py:attr:`keystore_password` must be given.
        """
        return self._keystore

    @keystore.setter
    def keystore(self, keystore: str):
        self._keystore = keystore

    @property
    def keystore_password(self):
        """
        str: The password of the keystore given in :py:attr:`keystore`.
        The same password is used to decrypt the the private key given in :py:attr:`client_private_key`.
        """
        return self._keystore_password

    @keystore_password.setter
    def keystore_password(self, keystore_password: str):
        self._keystore_password = keystore_password

    @property
    def ssl_protocol(self):
        """
        str: The SSL protocol for SSL connections. The default value is ``TLSv1.2``.
        """
        return self._ssl_protocol

    @ssl_protocol.setter
    def ssl_protocol(self, ssl_protocol: str):
        self._ssl_protocol = ssl_protocol

    @property
    def server_uri(self):
        """
        str: The URI of the MQTT server, either
        ``tcp://<hostid>[:<port>]`` or ``ssl://<hostid>[:<port>]``.
        The port defaults to 1883 for "tcp:" and 8883 for "ssl:" URIs.
        """
        return self._server_uri

    @server_uri.setter
    def server_uri(self, server_uri: str):
        self._server_uri = server_uri

    @property
    def reconnection_bound(self):
        """
        int: Number of reconnection attempts in case of connect failures.
        Specify 0 for no retry, a non-zero positive number *n* for *n* retries, or -1 for inifinite retry.
        """
        return self._reconnection_bound

    @reconnection_bound.setter
    def reconnection_bound(self, reconnection_bound: int):
        if reconnection_bound < -1:
            raise ValueError(reconnection_bound)
        self._reconnection_bound = reconnection_bound

    @property
    def keep_alive_seconds(self):
        """
        int: Automatically generate an MQTT ping message to the server if a message or ping hasn't been
        sent or received in the last keelAliveInterval seconds. 
        Enables the client to detect if the server is no longer available
        without having to wait for the TCP/IP timeout. A value of 0 disables keepalive processing.
        The default is 60.
        """
        return self._keep_alive_seconds

    @keep_alive_seconds.setter
    def keep_alive_seconds(self, keep_alive_seconds: int):
        if keep_alive_seconds < 0:
            raise ValueError(keep_alive_seconds)
        self._keep_alive_seconds = keep_alive_seconds

    @property
    def command_timeout_millis(self):
        """
        int: The maximum time in milliseconds to wait for an MQTT connect or publish action to complete.
        A value of 0 causes the client to wait infinitely.
        The default is 0.
        """
        return self._command_timeout_millis

    @command_timeout_millis.setter
    def command_timeout_millis(self, command_timeout_millis: int):
        if command_timeout_millis < 0:
            raise ValueError(command_timeout_millis)
        self._command_timeout_millis = command_timeout_millis

    @property
    def client_id(self):
        """
        str: A unique identifier for a connection to the MQTT server. The MQTT broker only allows a single
        connection for a particular clientID. By default a unique client ID is automatically
        generated.
        """
        return self._client_id

    @client_id.setter
    def client_id(self, client_id: str):
        self._client_id = client_id

    def _check_types(self):
        if self._trusted_certs is not None:
            # the setter ensures that a string is converted to a one element list
            for c in self._trusted_certs:
                if not isinstance(c, str):
                    raise TypeError('trusted_certs must be of type str or list of str')
        if self._vm_arg is not None:
            if not isinstance(self._vm_arg, str) and not isinstance(self._vm_arg, list):
                raise TypeError('vm_arg must be of type str or list of str')
            if isinstance(self._vm_arg, list):
                for v in self._vm_arg:
                    if not isinstance(v, str):
                        raise TypeError('vm_arg must be of type str or list of str')
    
    def _check_adjust(self):
        if self._truststore:
            if not self._truststore_password:
                raise ValueError('the truststore property requires the truststore_password property to be set')
        if self._client_cert:
            if not self._client_private_key:
                raise ValueError('the client_cert property requires the client_private_key property to be set')
        if self._keystore:
            if not self._keystore_password:
                raise ValueError('the keystore property requires the keystore_password property to be set')
        if not self._server_uri:
            raise ValueError('the server_uri property is required.')

    def create_spl_params(self, topology) -> dict:
        spl_params = dict()
        if self.trusted_certs:
            if self.truststore:
                print("adding trusted certificate(s) to truststore: " + self.truststore)
                extend_truststore(self.trusted_certs, self.truststore, self.truststore_password)
            else:
                # create truststore with given certificates
                truststore_basename = 'truststore-' + _generate_random_digits(16) + '.jks'
                truststore_filepath = os.path.join(gettempdir(), truststore_basename)
                print("adding trusted certificate(s) to new truststore: " + truststore_filepath)
                truststore_pass = create_truststore(self.trusted_certs, truststore_filepath)
                print("truststore password is: " + truststore_pass)
                topology.add_file_dependency(truststore_filepath, 'etc')
                spl_params['trustStore'] = 'etc/' + truststore_basename
                spl_params['trustStorePassword'] = truststore_pass

        if self.truststore:
            topology.add_file_dependency(self.truststore, 'etc')
            spl_params['trustStore'] = 'etc/' + os.path.basename(self.truststore)
            spl_params['trustStorePassword'] = self.truststore_password
       
        if self.client_cert:
            if self.keystore:
                print("adding client cert and key to keystore: " + self.keystore)
                extend_keystore(self.client_cert, self.client_private_key, self.keystore, self.keystore_password)
            else:
                # create keystore with given certificate and key
                keystore_basename = 'keystore-' + _generate_random_digits(16) + '.jks'
                keystore_filepath = os.path.join(gettempdir(), keystore_basename)
                print("adding client certificate and key to new keystore: " + keystore_filepath)
                keystore_pass = create_keystore(self.client_cert, self.client_private_key, keystore_filepath)
                print("keystore password is: " + keystore_pass)
                topology.add_file_dependency(keystore_filepath, 'etc')
                spl_params['keyStore'] = 'etc/' + keystore_basename
                spl_params['keyStorePassword'] = keystore_pass
        
        if self.keystore:
            topology.add_file_dependency(self.keystore, 'etc')
            spl_params['keyStore'] = 'etc/' + os.path.basename(self.keystore)
            spl_params['keyStorePassword'] = self.keystore_password
    
        spl_params['serverURI'] = self.server_uri
        spl_params['keepAliveInterval'] = self.keep_alive_seconds
        spl_params['reconnectionBound'] = self.reconnection_bound
        if self.reconnection_bound != 0:
            spl_params['period'] = streamsx.spl.types.int64(30)
        
        if self.app_config_name:
            spl_params['appConfigName'] = self.app_config_name
            spl_params['passwordPropName'] = MQTTComposite._APP_CONFIG_PROP_NAME_FOR_PASSWORD
            spl_params['userPropName'] = MQTTComposite._APP_CONFIG_PROP_NAME_FOR_USERNAME
        if self.username:
            spl_params['userID'] = self.username
        if self.password:
            spl_params['password'] = self.password
        if self.client_id:
            spl_params['clientID'] = self.client_id
        if self.command_timeout_millis is not None:
            spl_params['commandTimeout'] = streamsx.spl.types.int64(self.command_timeout_millis)
        if self.ssl_protocol:
            spl_params['sslProtocol'] = self.ssl_protocol
        if self.vm_arg or self.ssl_debug:
            vmargs = []
            if isinstance(self.vm_arg, list):
                vmargs.extend(self.vm_arg)
            elif isinstance(self.vm_arg, str):
                vmargs.append(self.vm_arg)
            if self.ssl_debug:
                vmargs.append('-Djavax.net.debug=all')
            spl_params['vmArg'] = vmargs
        return spl_params


class MQTTSink(MQTTComposite, AbstractSink):
    """
    The ``MQTTSink`` represents a stream termination that publishes messages to one or more MQTT topics.
    Instances can be passed to ``Stream.for_each()`` to create a sink (stream termination).
    
    Args:
        server_uri(str): The MQTT server URI
        topic(str): The topic to publish the messages to. Mutually exclusive with ``topic_attribute_name``.
        topic_attribute_name(str): The name of a tuple attribute denoting the destination topic.
            Mutually exclusive with ``topic``.
        data_attribute_name(str): The name of the tuple attribute containing the message data to be published. ``data`` is assumed as default.
        **options(kwargs): optional parameters as keyword arguments
    """
    def __init__(self, server_uri, topic=None, topic_attribute_name=None, data_attribute_name=None, **options):
        MQTTComposite.__init__(self, **options)
        AbstractSink.__init__(self)
        if not topic and not topic_attribute_name:
            raise ValueError('One of topic or topic_attribute_name is required')
        if topic and topic_attribute_name:
            raise ValueError('Only one of topic or topic_attribute_name is allowed')
        if not server_uri:
            raise ValueError(server_uri)
        self.server_uri = server_uri
        
        self._retain = False
        self._topic = topic
        self._topic_attribute_name = topic_attribute_name
        self._data_attribute_name = data_attribute_name
        self._qos = None
        if 'qos' in options:
            self.qos = options.get('qos')
        if 'retain' in options:
            self.retain = options.get('retain')
        self._op = None

    def create_spl_params(self, topology) -> dict:
        spl_params = MQTTComposite.create_spl_params(self, topology)
        if self.qos is not None:
           spl_params['qos'] = self.qos

        if self._topic:
            spl_params['topic'] = self._topic
        if self._topic_attribute_name:
            spl_params['topicAttributeName'] = self._topic_attribute_name
        if self._retain:
            spl_params['retain'] = self._retain
        #verify that we do not setup invalid SPL parameters
        for paramName in spl_params.keys():
            if not paramName in _MqttSink.SUPPORTED_SPL_PARAMS:
                raise AttributeError('illegal operator parameter: {}'.format(paramName))
        return spl_params

    @property
    def qos(self):
        """
        int: qos value for publishing messages. Allowed values are 0, 1, and 2.
        """
        return self._qos

    @qos.setter
    def qos(self, qos):
        if qos is not None:
            if not isinstance(qos, int):
                raise TypeError(qos)
            if 0 <= qos <= 2:
                pass
            else:
                raise ValueError('qos must be 0, 1, or 2')
        self._qos = qos

    @property
    def retain(self):
        """
        bool: Indicates if messages should be retained on the MQTT server if there is no subscription for the topic. The default is ``False``.
        """
        return self._retain

    @retain.setter
    def retain(self, retain: bool):
        self._retain = retain

    def populate(self, topology, stream, name, **options):
        self._check_types()
        self._check_adjust()
        spl_params = self.create_spl_params(topology)
        #derive 'dataAttributeName' from schema
        schema = stream.oport.schema
        if schema is CommonSchema.Python:
            spl_params['dataAttributeName'] = '__spl_po'
            raise TypeError('CommonSchema.Python is not supported by the MQTTSink')
        elif schema is CommonSchema.XML:
            spl_params['dataAttributeName'] = 'document'
            raise TypeError('CommonSchema.XML is not supported by the MQTTSink')
        elif schema is CommonSchema.Json:
            spl_params['dataAttributeName'] = 'jsonString'
        elif schema is CommonSchema.String:
            spl_params['dataAttributeName'] = 'string'
        elif schema is CommonSchema.Binary:
            spl_params['dataAttributeName'] = 'binary'
        # TODO add more pre-defined schemas
        else:
            if self._data_attribute_name:
                spl_params['dataAttributeName'] = self._data_attribute_name

        self._op = _MqttSink(stream, spl_params, name)
        return streamsx.topology.topology.Sink(self._op)


class MQTTSource(MQTTComposite, AbstractSource):
    """
    Represents a source for messages read from an MQTT server, which can be passed to 
    ``Topology.source()`` to create a stream.
    
    Args:
        server_uri(str): The MQTT server URI
        topics(str|list): The topic or topics to subscribe for messages.
        schema: The schema of the created stream
        data_attribute_name(str): The name of the tuple attribute containing the message data. ``data`` is assumed as default.
        topic_attribute_name(str): The name of a tuple attribute denoting the source topic of received messages.
        **options(kwargs): optional parameters as keyword arguments
    """
    
    def __init__(self, server_uri, topics, schema, data_attribute_name=None, topic_attribute_name=None, **options):
        MQTTComposite.__init__(self, **options)
        AbstractSource.__init__(self)
        if not topics:
            raise ValueError(topics)
        if not server_uri:
            raise ValueError(server_uri)
        if not schema:
            raise ValueError(schema)
        self.server_uri = server_uri
        self._schema = schema
        self._topics = topics
        self._topic_attribute_name = topic_attribute_name
        self._data_attribute_name = data_attribute_name
        self._qos = None
        self._message_queue_size = 500
        if 'qos' in options:
            self.qos = options.get('qos')
        if 'message_queue_size' in options:
            self.message_queue_size = options.get('message_queue_size')
        self._op = None
        
    @property
    def qos(self):
        """
        int|list: qos values for the messages to subscribe.
        
        Example::
        
            mqttSource = MQTTSource('tcp://host.domain:1883', ['topic1', 'topic2'], CommonSchema.String)
            mqttSource.qos = 2

        The qos value can also be a list of integers::
        
            mqttSource = MQTTSource('tcp://host.domain:1883', ['topic1', 'topic2'], CommonSchema.String)
            mqttSource.qos = [1, 2]
        """
        return self._qos

    @qos.setter
    def qos(self, qos):
        # qos in [0..2]
        if qos is not None:
            if isinstance(qos, int):
                if 0 <= qos <= 2:
                    pass
                else:
                    raise ValueError('qos must be 0, 1, or 2')
                self._qos = qos
            elif isinstance(qos, list):
                for q in qos:
                    if isinstance(q, int):
                        if 0 <= q <= 2:
                            pass
                        else:
                            raise ValueError('qos must be 0, 1, or 2')
                    else:
                        raise TypeError('qos must be int or list of int')
                if qos:
                    # non emty list
                    self._qos = qos
                else:
                    raise ValueError('qos must not be an empty list')
            else:
                raise TypeError(qos)
        else:
            self._qos = qos

    @property
    def message_queue_size(self):
        """
        int: The size, in number of messages, of an internal receive buffer.
        The receiver stops fetching messages when the buffer is full.
        The default is 500.
        """
        return self._message_queue_size

    @message_queue_size.setter
    def message_queue_size(self, message_queue_size: int):
        if message_queue_size < 1:
            raise ValueError(message_queue_size)
        self._message_queue_size = message_queue_size

    def create_spl_params(self, topology) -> dict:
        spl_params = MQTTComposite.create_spl_params(self, topology)
        if isinstance(self.qos, int):
            spl_params['qos'] = self.qos
        elif isinstance(self.qos, list):
             # topology converts list of int to SPL list of rstring; use the qosStr parameter as workaround
            spl_params['qos'] = streamsx.spl.op.Expression.expression(','.join([str(qos) for qos in self.qos]))
        if self._topics:
            spl_params['topics'] = self._topics
            
        if self._topic_attribute_name:
            spl_params['topicOutAttrName'] = self._topic_attribute_name
        spl_params['messageQueueSize'] = self._message_queue_size
        #verify that we do not setup invalid SPL parameters
        for paramName in spl_params.keys():
            if not paramName in _MqttSource.SUPPORTED_SPL_PARAMS:
                raise AttributeError('illegal operator parameter: {}'.format(paramName))
        return spl_params

    def populate(self, topology, name, **options):
        self._check_types()
        self._check_adjust()
        spl_params = self.create_spl_params(topology)
        #derive 'dataAttributeName' from schema
        if self._schema is CommonSchema.Python:
            spl_params['dataAttributeName'] = '__spl_po'
            raise TypeError('CommonSchema.Python is not supported by the MQTTSource')
        elif self._schema is CommonSchema.XML:
            spl_params['dataAttributeName'] = 'document'
            raise TypeError('CommonSchema.XML is not supported by the MQTTSource')
        elif self._schema is CommonSchema.Json:
            spl_params['dataAttributeName'] = 'jsonString'
        elif self._schema is CommonSchema.String:
            spl_params['dataAttributeName'] = 'string'
        elif self._schema is CommonSchema.Binary:
            spl_params['dataAttributeName'] = 'binary'
        # TODO add more pre-defined schemas
        else:
            if self._data_attribute_name:
                spl_params['dataAttributeName'] = self._data_attribute_name

        self._op = _MqttSource(topology, self._schema, spl_params, name)
        return self._op.outputs[0]


class _MqttSource(streamsx.spl.op.Source):

    SUPPORTED_SPL_PARAMS = set(['topics', 'appConfigName', 'clientID',
                                'commandTimeout', 'connection', 'connectionDocument',
                                'dataAttributeName', 'keepAliveInterval', 'keyStore',
                                'keyStorePassword', 'messageQueueSize', 'password',
                                'passwordPropName', 'period', 'qos', 'qosStr',
                                'reconnectionBound', 'serverURI', 'sslProtocol',
                                'topicOutAttrName', 'trustStore', 'trustStorePassword',
                                'userID', 'userPropName', 'vmArg'])
    
    def __init__(self, topology, schema, spl_params, name=None):
        kind="com.ibm.streamsx.mqtt::MQTTSource"
        schemas = schema
        super(_MqttSource, self).__init__(topology, kind, schemas, spl_params, name)


class _MqttSink(streamsx.spl.op.Sink):
    
    SUPPORTED_SPL_PARAMS = set(['appConfigName', 'clientID', 'commandTimeout', 'connection',
                                'connectionDocument', 'dataAttributeName', 'keepAliveInterval',
                                'keyStore', 'keyStorePassword', 'password', 'passwordPropName',
                                'period', 'qos', 'qosAttributeName', 'reconnectionBound', 'retain',
                                'serverURI', 'sslProtocol', 'topic', 'topicAttributeName',
                                'trustStore', 'trustStorePassword', 'userID', 'userPropName', 'vmArg'])

    def __init__(self, stream, spl_params, name=None):
        kind = "com.ibm.streamsx.mqtt::MQTTSink"
        super(_MqttSink, self).__init__(kind, stream, spl_params, name)

