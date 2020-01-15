# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import streamsx.spl.op
import streamsx.spl.types

"""
    Configuration properties apply to mqtt_source() and mqtt_sink() 
    unless stated otherwise.
    These properties have to be provided in a Python [dict] to the two
    functions.

    appConfigName
        This parameter specifies the name of application configuration that
        stores client credential information, the credential specified via 
        application configruation overrides the one specified with userID and 
        password parameters. 

  
    clientID
        Optional String. A unique identifier for a connection
        to the MQTT server.
        he MQTT broker only allows a single
        onnection for a particular  clientID.
        By default a unique client ID is automatically
        generated for each use of  publish() and subscribe().
        The specified clientID is used for the first
        publish() or subscribe() use and
        suffix is added for each subsequent uses.
    
    commandTimeout
        Optional Long. The maximum time in milliseconds
        to wait for a MQTT connect or publish action to complete.
        A value of 0 causes the client to wait indefinitely.
        The default is 0.
    
    keepAliveInterval
        Optional Integer.  Automatically generate a MQTT
        ping message to the server if a message or ping hasn't been
        sent or received in the last keelAliveInterval seconds.  
        Enables the client to detect if the server is no longer available
        without having to wait for the TCP/IP timeout.  
        A value of 0 disables keepalive processing.
        The default is 60.
    
    keyStore
        Optional String. The pathname to a file containing the
        MQTT client's public private key certificates.
        If a relative path is specified, the path is relative to the
        application directory. 
        Required when an MQTT server is configured to use SSL client authentication.
    
    keyStorePassword
        Required String when keyStore is used.
        The password needed to access the encrypted keyStore file.

    messageQueueSize
        [mqtt_source] Optional Integer. The size, in number
        of messages, of the subscriber's internal receive buffer.  Received
        messages are added to the buffer prior to being converted to a
        stream tuple. The receiver blocks when the buffer is full.
        The default is 50.

    password
        Optional String.  The identifier to use when authenticating
        with server configured to require that form of authentication. 

    passwordPropertyName
        This parameter specifies the property name of password in the application
        configuration. If the appConfigName parameter is specified and the 
        passwordPropName parameter is not set, a compile time error occurs.    
    
    period
        Optional Long. The time in milliseconds before
        attempting to reconnect to the server following a connection failure.
        The default is 60000.
    
    qos
        Optional Integer. The default
        MQTT quality of service used for message handling.
        The default is 0.

    qosAttributeName
        Attribute name that contains the qos to publish the message with. 
        This parameter is mutually exclusive with the "qos" parameter. 

    reconnectionBound
        This optional parameter of type int32 specifies the number 
        of successive connections that are attempted for an operator. 
        Specify 0 for no retry, n for n number of retries, -1 for inifinite retry.

    retain
        [mqtt_sink] Optional Boolean. Indicates if messages should be
        retained on the MQTT server.  Default is false.
    
    serverURI
        Required String. URI to the MQTT server, either
        tcp://<hostid>[:<port>]}
        or ssl://<hostid>[:<port>]}.
        The port defaults to 1883 for "tcp:" and 8883 for "ssl:" URIs.

    sslProtocol
        This optional parameter of type rstring specifies the ssl protocol to 
        use for making SSL connections. If this parameter is not specified, 
        the default protocol 'TLSv1.2' will be used. 

    trustStore
        Optional String. The pathname to a file containing the
        public certificate of trusted MQTT servers.  If a relative path
        is specified, the path is relative to the application directory.
        Required when connecting to a MQTT server with an 
        ssl:/... serverURI.
    
    trustStorePassword
        Required String when trustStore is used.
        The password needed to access the encrypted trustStore file.

    userID
        Optional String.  The identifier to use when authenticating
        with a server configured to require that form of authentication.
    
    userPropName
        This parameter specifies the property name of user name in the 
        application configuration. If the appConfigName parameter is 
        specified and the userPropName parameter is not set, a compile time error occurs

"""

def mqtt_sink(stream,
              config,                                # mandatory, either containing appConfig name or all needed parameters
              topic = None,                          # mandatory but mutual exclive with 'topic_attribute_name'
              topic_attribute_name = None,           # mandatory but mutual exclive with 'topic'
              data_attribute_name = None,            # default: (only 1 attribute in schema ? attribute name : 'data')
              name = None                     
              ):
    """
    Creates a MQTT client for sending data to a MQTT server.

    Parameter: 
        stream
            mandatory
            stream of tuples which should be processed
        config
            mandatory
            general MQTT connection configuration
        topic
            mandatory but (mutual exclusive with 'topic_attribute_name')
            topic to which all tuples data of the stream 
            should be sent 
        topic_attribute_name
            mandatory but (mutual exclusive with 'topic')
            attribute in each tuple which defines the topic 
            the tuple data should be sent to
        data_attribute_name
            optional
            name of the tuple attribute containing the data which
            should be sent, if not specified: a one attribute tuple
            will send this attribute, when multiple attributes are 
            in the tuple a 'data' attribute will be used
        name  
            optional 
            name of the operator (used in visualization and logging)

    Returns:
        None

    Hints:
        Not the whole tuple but only one attribute (defined as the data to be sent). 
        So the input stream has to contain at least this one attribute. But it can 
        have multiple attributes which are ignored. In case parameter 
        'topic_attribute_name' is used this attribute defines the topic the tuples 
        data should be sent to. This attribute has to be of a string type. As the 
        underlying operator is requires a structured tuple. The input stream schema
        has to be structured.

    Example::

        class MqttDataTuple(typing.NamedTuple):
            topic_name: str
            data:       str

        topo = Topology(name)
        mqtt_config = {}
        mqtt_config[serverURI]  = 'ssl://my.mqtt.broker'
        mqtt_config['userID']   = 'xxxx'
        mqtt_config['password'] = 'xxxx'
     
        ...
        # in this scenario your stream need to be a structured schema type
        # with attribute 'data' containing the data to be sent
        # e.g. the above type 'MqttDataTuple'
    
        # create some streaming content
        sink_stream = create_your_stream(topo)    

        # send content of attribute 'data' (default) to topic 'test_topic' 
        # to the mqtt_broker configured in mqtt_config
        # additional attributes in the tuples will be ignored 
        mqtt.mqtt_sink(sink_stream,mqtt_config,topic='test_topic')

        # subscribe to topic 'test_topic' on the mqtt_broker configured
        # in mqtt_config
        # generate a stream with schema of class MqttDataTuple
        # message data is written to (default) 'data' attribute, the topic 
        # the message was received from is written to attribute 'topic_name'
        source_stream = mqtt.mqtt_source(topo, [MqttDataTuple], mqtt_config, 'test_topic', topic_attribute_name = 'topic_name')

    """

    params = config.copy()

    if data_attribute_name is not None:
        params['dataAttributeName'] = data_attribute_name
    if (topic is not None) and (topic_attribute_name is not None):
        raise ValueError("Invalid topic configuration: Set either 'topic' or 'topic_attribute_name' parameter.")
    if (topic is None) and (topic_attribute_name is None):
        raise ValueError("No topic configuration: Set either 'topic' or 'topic_attribute_name' parameter.")
    if (topic is not None):
        params['topic'] = topic
    if (topic_attribute_name is not None):
        params['topicAttributeName'] = topic_attribute_name

    _op = _MqttSink(stream,params,name)

    return None


def mqtt_source(topology,                            # mandatory, topology the source should be added to
              schema,                                # mandatory, output schema
              config,
              topics,                                # mandatory, list of topics to subscribe to
              topic_attribute_name = None,           # attribute holding the topic of the message
              data_attribute_name = None,            # default: only 1 attribute in schema?it's name:look for 'data' attribute
              name = None               
              ):
    """
    Creates a MQTT client for receiving data (topology source) by subscribing to topics
    on a MQTT server.

    Parameter: 
        topology
            mandatory
            the topology this data source is added to
        schema 
            mandatory
            a schema definition for the type of the stream providing
            the received data
        config
            mandatory 
            general MQTT connection configuration
        topics
            mandatory
            list of topics this MQTT client is subscribing to 
        topic_attribute_name
            optional
            attribute in the schema which will contain the topic the
            tuple data was received from
        data_attribute_name
            optional
            name of the tuple attribute containing the data 
            if not specified: if the schema contains only one attribute
            this one will contain the data, when multiple attributes are 
            in the schema a 'data' attribute will be used
        name  
            optional 
            name of the operator (used in visualization and logging)

    Returns:
        Stream (of type 'schema') of received data

    Example::

        class MqttDataTuple(typing.NamedTuple):
            topic_name: str
            data:       str

        topo = Topology(name)
        mqtt_config = {}
        mqtt_config[serverURI]  = 'ssl://my.mqtt.broker'
        mqtt_config['userID']   = 'xxxx'
        mqtt_config['password'] = 'xxxx'
        ...
        # in this scenario your stream need to be a structured schema type
        # with attribute 'data' containing the data to be sent
        # e.g. the above type 'MqttDataTuple'
    
        # create some streaming content
        sink_stream = create_your_stream(topo)    

        # send content of attribute 'data' (default) to topic 'test_topic' 
        # to the mqtt_broker configured in mqtt_config
        # additional attributes in the tuples will be ignored 
        mqtt.mqtt_sink(sink_stream,mqtt_config,topic='test_topic')

        # subscribe to topic 'test_topic' on the mqtt_broker configured
        # in mqtt_config
        # generate a stream with schema of class MqttDataTuple
        # message data is written to (default) 'data' attribute, the topic 
        # the message was received from is written to attribute 'topic_name'
        source_stream = mqtt.mqtt_source(topo, [MqttDataTuple], mqtt_config, 'test_topic', topic_attribute_name = 'topic_name')

    """


    params = config.copy()
    # retain is for MQTT msg publish only
    if ('retain' in params):
        del params['retain']
    if (topics is not None):
        params['topics'] = topics
    else:
        raise ValueError("Invalid topic configuration: You must set a topic to subscribe to.")
    if (topic_attribute_name is not None):
        params['topicOutAttrName'] = topic_attribute_name
    if (data_attribute_name is not None):
        params['dataAttributeName'] = data_attribute_name

    _operator = _MqttSource(topology,schema,params,name)
    
    return _operator.outputs[0]


class _MqttSource(streamsx.spl.op.Source):
    def __init__(self, 
              topology, 
              schema,
              config,                           # configuration dictionary to hold all parameters of the SPL operator
              name=None):

        kind="com.ibm.streamsx.mqtt::MQTTSource"
        super(_MqttSource, self).__init__(topology,kind,schema,config,name)

class _MqttSink(streamsx.spl.op.Sink):
    def __init__(self, 
              stream, 
              config,                                # configuration dictionary to hold all parameters of the SPL operator
              name=None):

        kind="com.ibm.streamsx.mqtt::MQTTSink"

        super(_MqttSink, self).__init__(kind,stream,config,name)

