from streamsx.mqtt import MQTTSource, MQTTSink
import streamsx.topology.context as context
from streamsx.topology.context import ContextTypes, JobConfig
from streamsx.topology.topology import Topology
from streamsx.topology.schema import CommonSchema

mqtt_server_uri = 'tcp://172.16.33.10:1883'
topology = Topology()
s = 'Each character will be an MQTT message'
data = topology.source([c for c in s]).as_string()
# publish to MQTT
data.for_each(MQTTSink(server_uri=mqtt_server_uri, topic='topic'),
              name='MQTTpublish')

# subscribe for data and print to stdout
received = topology.source(MQTTSource(mqtt_server_uri, schema=CommonSchema.String, topics='topic'))
received.print()

job_config = JobConfig(job_name='MQTTBasic', tracing='info')
submission_config = {}
# add the job_config into the submission config: 
job_config.add(submission_config)
print('submission_config: ' + str(submission_config))
context.submit(ContextTypes.DISTRIBUTED, topology, config=submission_config)
# the Streams Job keeps running and must be cancelled manually
