from setuptools import setup
import streamsx.mqtt
setup(
    name='streamsx.mqtt',
    packages=['streamsx.mqtt'],
    include_package_data=True,
    version=streamsx.mqtt.__version__,
    description='MQTT integration for IBM Streams topology applications',
    long_description=open('DESC.txt').read(),
    author='IBM Streams @ github.com',
    author_email='rolef.heinrich@de.ibm.com',
    license='Apache License - Version 2.0',
    url='https://github.com/IBMStreams/pypi.streamsx.mqtt',
    keywords=['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'mqtt'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    install_requires=['streamsx>=1.14.3a0,<2.0', 'streamsx.toolkits>=1.2.0'],

    test_suite='nose.collector',
    tests_require=['nose']
)
