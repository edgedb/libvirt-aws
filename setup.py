from setuptools import setup


setup(
    setup_requires=[
        'setuptools_scm',
    ],
    use_scm_version=True,
    name='libvirt-ebs',
    description='Amazon EBS-compatible API server for libvirt',
    author='EdgeDB Inc.',
    author_email='hello@edgedb.com',
    packages=['libvirt_ebs'],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'libvirt_ebs = libvirt_ebs.main:main',
        ]
    },
    install_requires=[
        'aiohttp~=3.7.0',
        'click~=7.1',
        'dicttoxml~=1.7.4',
        'libvirt-python~=7.2.0',
        'xmltodict~=0.12.0',
    ],
)
