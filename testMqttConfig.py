from streamsx.mqtt import MQTTSource, MQTTSink
from streamsx.topology import context
from streamsx.topology.topology import Topology
from streamsx.topology.schema import CommonSchema

server_uri = 'tcp://server:1883'
sk = MQTTSink(server_uri=server_uri, 
             #topic="myTopic",
             topic_attribute_name="topiC",
             data_attribute_name='data'
             )
sk.reconnection_bound=5
#sk.qos=[1,2]
sk.qos=2
#sk.qos=None
sk.trusted_certs=['/tmp/secrets/cluster_ca_cert.pem', '/tmp/secrets/cluster-ca.crt']
sk.truststore = "/tmp/truststore-3828273497884343.jks"
sk.truststore_password = 'OKhcla3GmJs1DLDu'
sk.client_cert='/home/rolef/infosphereStreams/development/tk/ws_el7_43/kafkaTransactionSupport/etc/amqstreams/rolef.crt'
sk.client_private_key='/home/rolef/infosphereStreams/development/tk/ws_el7_43/kafkaTransactionSupport/etc/amqstreams/rolef.key'

sk.keystore = "/tmp/keystore-9031209600785916.jks"
sk.keystore_password = '772kS7ZmvmsjFseB'
sk.ssl_protocol = 'TLSv1.2'
sk.vm_arg = ["-Xmx1G"]
sk.ssl_debug = True
sk.app_config_name = "abbconf"
sk.client_id = "client-ID"
sk.command_timeout_millis=47
sk.keep_alive_seconds = 3
sk.password = "passw0rd"
sk.username = "rolef"
sk.retain = True
#c.message_queue_size=23


src = MQTTSource(server_uri, 
                 topics=['t1', 't2'],
                 schema=CommonSchema.String,
                 data_attribute_name="xyz",
                 topic_attribute_name="bla")
#src.qos = 1
a = 4 - 3
b = a + 1
print('a=' + str(a))
print('b=' + str(b))
src.qos = [a,b]
src.message_queue_size = 122
src.client_id = "client-IDsrc"
src.reconnection_bound=-1
src.trusted_certs=['/tmp/secrets/cluster_ca_cert.pem', '/tmp/secrets/cluster-ca.crt']
#src.truststore = "/tmp/truststore-4657686007309004.jks"
src.truststore_password = '8Jan4FtpsoSSei5R'
src.client_cert='/home/rolef/infosphereStreams/development/tk/ws_el7_43/kafkaTransactionSupport/etc/amqstreams/rolef.crt'
src.client_private_key='/home/rolef/infosphereStreams/development/tk/ws_el7_43/kafkaTransactionSupport/etc/amqstreams/rolef.key'
#src.keystore = "/tmp/keystore-8728817490056367.jks"
src.keystore_password = 'YbJb34014jlREkF6'
src.ssl_protocol = 'TLSv1.1'
src.vm_arg = ["-Xmx13G"]
src.ssl_debug = True
src.app_config_name = "abbconf2"
src.command_timeout_millis=247
src.keep_alive_seconds = 23
src.password = "passw0rd2"
src.username = "rolef2"


topo = Topology()
#data = topo.source(["A","B","C","D","E","F","G","H","I","J"]).as_string()

data = topo.source(src, "MQTTsubscribe")
data.for_each(sk, name="MQTTpublish")

#submission = context.submit(context.ContextTypes.TOOLKIT, topo)
submission = context.submit(context.ContextTypes.BUNDLE, topo)
#c._check_types()
#c._check_adjust()
#print(sk.__dict__)
#print(sk._config.__dict__)
print(submission)