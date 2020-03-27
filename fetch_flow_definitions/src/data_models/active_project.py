class ActiveProject(object):
    def __init__(self, project_name, rapid_pro_domain, rapid_pro_token_url, flow_definitions_upload_url_prefix):
        self.project_name = project_name
        self.rapid_pro_domain = rapid_pro_domain
        self.rapid_pro_token_url = rapid_pro_token_url
        self.flow_definitions_upload_url_prefix = flow_definitions_upload_url_prefix

    @classmethod
    def from_dict(cls, source):
        project_name = source["project_name"]
        rapid_pro_domain = source["rapid_pro_domain"]
        rapid_pro_token_url = source["rapid_pro_token_url"]
        flow_definitions_upload_url_prefix = source.get("flow_definitions_upload_url_prefix")

        return cls(project_name, rapid_pro_domain, rapid_pro_token_url, flow_definitions_upload_url_prefix)
