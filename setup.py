from setuptools import setup, find_packages

setup(
    name="backuptool",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
    entry_points={
        'console_scripts': [
            'backuptool=backuptool.cli:main',
        ],
    },
)
