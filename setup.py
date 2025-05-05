import os
from glob import glob
from setuptools import setup

package_name = 'rosbag_overlap'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[('share/ament_index/resource_index/packages',
                 ['resource/' + package_name]),
                (os.path.join('share', package_name), ['package.xml']),
                (os.path.join('share', package_name,
                              'launch'), glob('launch/*launch.[pxy][yma]*')),
                (os.path.join('share', package_name,
                              'config'), glob('config/*'))],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='root',
    maintainer_email='lichtenfeld@sim.tu-darmstadt.de',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts':
        ['rosbag_overlap = rosbag_overlap.rosbag_overlap:main',
         'split = rosbag_overlap.split:main',],
    },
)
