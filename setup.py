from setuptools import setup, find_packages

long_description = """
Brainer is a simple distributed caching mechanism done in 48 hours.
"""


setup(
    name="brainer",
    version='0.1',
    description="Distributed Cache with Twisted and ZeroMQ",
    long_description=long_description,
    author="Nicholas Amorim",
    author_email="nicholas@alienretro.com",
    url="https://github.com/nicholasamorim/brainer",
    license="MIT",
    packages=find_packages(),
    install_requires=['twisted', 'pyzmq', 'txZMQ', 'u-msgpack-python'],
    # requires=['twisted(==15.3.0)', 'pyzmq', 'txZMQ', 'u-msgpack-python'],
    tests_require=['twisted', 'pyzmq', 'txZMQ', 'u-msgpack-python', 'mock'],
    keywords='distributed cache twisted zeromq',
    scripts=['brainer/brainer-cli', 'brainer/run_broker', 'brainer/run_node'],
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Twisted",
        "Operating System :: POSIX :: Linux",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
    ],
)