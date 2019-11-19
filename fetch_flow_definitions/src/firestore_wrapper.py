import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials, firestore
from src.data_models import ActiveProject

log = Logger(__name__)


class FirestoreWrapper(object):
    def __init__(self, cert):
        """
        :param cert: Path to a certificate file or a dict representing the contents of a certificate.
        :type cert: str or dict
        """
        cred = credentials.Certificate(cert)
        firebase_admin.initialize_app(cred)
        self.client = firestore.client()

    def _get_active_projects_collection_ref(self):
        return self.client.collection(f"active_projects")

    def get_active_projects(self):
        """
        Downloads all the active projects from Firestore.

        :return: list of active projects.
        :rtype: list of ActiveProject
        """
        log.info("Downloading the list of active projects from Firestore...")
        active_projects = []
        for doc in self._get_active_projects_collection_ref().get():
            active_projects.append(ActiveProject.from_dict(doc.to_dict()))
        log.info(f"Downloaded {len(active_projects)} active projects from Firestore")
        return active_projects
