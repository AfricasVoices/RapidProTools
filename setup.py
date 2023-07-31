from setuptools import setup

setup(
    name="RapidProTools",
    version="0.3.9",
    url="https://github.com/AfricasVoices/RapidProTools",
    packages=["rapid_pro_tools"],
    install_requires=["rapidpro-python~2", "python-dateutil~2",
                      "coredatamodules @ git+https://github.com/AfricasVoices/CoreDataModules"]
)
