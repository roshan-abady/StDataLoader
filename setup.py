from setuptools import setup, find_packages

setup(
    name='myob_data_loader',
    version='1.0',
    packages=find_packages(),
    install_requires=[line.strip() for line in open("requirements.txt", "r").readlines()],
    entry_points={
        'console_scripts': [
            'myob_data_loader = main.main:main'
        ]
    }
)
