from setuptools import setup

setup(
    name="RapidProTools",
    version="0.3.4",
    url="https://github.com/AfricasVoices/RapidProTools",
    packages=["rapid_pro_tools"],
    install_requires=["rapidpro-python", "python-dateutil",
                      "coredatamodules @ git+https://github.com/AfricasVoices/CoreDataModules"]
)
