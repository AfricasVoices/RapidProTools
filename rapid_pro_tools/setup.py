from setuptools import setup

setup(
    name="RapidProTools",
    version="0.0.0",
    url="https://github.com/AfricasVoices/RapidProTools",
    packages=["rapid_pro_tools"],
    install_requires=["core_data_modules"],
    dependency_links=["git+https://git@github.com/AfricasVoices/CoreDataModules.git#egg=core_data_modules"]
)
