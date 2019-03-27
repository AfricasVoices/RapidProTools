from setuptools import setup

setup(
    name="RapidProTools",
    version="0.0.0",
    url="https://github.com/AfricasVoices/RapidProTools",
    packages=["rapid_pro_tools"],
    install_requires=["CoreDataModules", "PipelineInfrastructure", "rapidpro-python", "python-dateutil"],
    dependency_links=[
        "git+https://git@github.com/AfricasVoices/CoreDataModules.git#egg=CoreDataModules",
        "git+https://git@github.com/AfricasVoices/Pipeline-Infrastructure.git#egg=Pipeline-Infrastructure",
    ]
)
